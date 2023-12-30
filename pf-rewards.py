import mysql.connector
import sys
import pandas as pd
from datetime import datetime, timedelta

def get_credentials():
    # Read the credentials and IP from the file
    with open('cred.txt', 'r') as file:
        username = file.readline().strip()  # Read the first line for username
        password = file.readline().strip()  # Read the second line for password
        db_ip = file.readline().strip()     # Read the third line for server IP
        db_name = file.readline().strip()   #Read fourth for db name

    # Connection configuration
    config = {
        'user': username,
        'password': password,
        'host': db_ip,  
        'port': 3306,
        'database': db_name,
        'raise_on_warnings': True
    }

    return config

def connect_to_db(config):
    try:
        # Establishing the connection to the database
        cnx = mysql.connector.connect(**config)
        print("Successfully connected to the database!")
        return cnx

    except mysql.connector.Error as err:
        print(f"Failed to connect: {err}")
        sys.exit(1)  # Exit the program

def fetch_data_to_dataframe(cursor, query):
    print("Executing SQL Statment...")
    # Execute the given SQL query
    cursor.execute(query)

    # Fetch all results
    result = cursor.fetchall()

    # Convert to DataFrame
    columns = [i[0] for i in cursor.description]  # this gets the column names
    df = pd.DataFrame(result, columns=columns)

    return df

def main():
    print("Starting Script")

    # Set the filename variable
    excel_file_name = "./export/pathfinder_data.xlsx"

    creds = get_credentials()

    db_connection = connect_to_db(creds)

    cursor = db_connection.cursor()

    #Do things here
    query = """
        SELECT 
            activity_log.characterId, 
            activity_log.year, 
            activity_log.week, 
            activity_log.active, 
            activity_log.mapId, 
            activity_log.connectionCreate, 
            `character`.id AS characterId, 
            `character`.name AS character_name,
            corporation.name AS corporation_name,
            corporation.ticker AS corporation_ticker,
            alliance.name AS alliance_name,
            alliance.ticker AS alliance_ticker
        FROM activity_log
        LEFT JOIN `character` ON activity_log.characterId = `character`.id
        LEFT JOIN corporation ON `character`.corporationId = corporation.id
        LEFT JOIN alliance ON `character`.allianceId = alliance.id;
    """
    df = fetch_data_to_dataframe(cursor, query)

    #Filter only on main MKUGA map (we should monitor this in case others are using our site)
    df = df[df['mapId'] == 1]

    # Drop the duplicate id columns from the DataFrame
    df = df.drop(columns=['characterId'])

    #Add in Week Ending calculated Fields, converts the Year and Week Number to the date the week ends on (Mondays per how pf counts).
    #Makes it easier to sort, etc
    df['Week Ending'] = df.apply(lambda row: datetime.strptime("01/02/" + str(row['year']), "%m/%d/%Y") + timedelta(weeks=int(row['week'])-1), axis=1)

    #Create the Weekly Summary Data (sums Connections Created and Active Players)
    weekly_summary = df.groupby(['year', 'week']).agg(
        connectionCreated_sum=('connectionCreate', 'sum'),
        active_sum=('active', 'sum')
    ).reset_index().rename(columns={'year': 'Year', 'week': 'Week Number'})

    # Add 'Week Ending' to weekly_summary
    weekly_summary['Week Ending'] = weekly_summary.apply(lambda row: datetime.strptime("01/02/" + str(row['Year']), "%m/%d/%Y") + 
                                                         timedelta(weeks=int(row['Week Number'])-1), axis=1)

    # Calculate total potential payout and amount over cap
    payout_per_connection = 1_000_000  # 1 million per connection baseline
    payout_cap = 125_000_000  # 125 million cap per week so 500mil every 4 weeks

    weekly_summary['total_payout'] = weekly_summary['connectionCreated_sum'] * payout_per_connection
    weekly_summary['amount_over'] = (weekly_summary['total_payout'] - payout_cap).clip(lower=0)
    
    # Calculate the payout adjustment multiplier
    # This will be 1 - (the percentage over cap)
    # Ensuring that it does not go below 0 and is 1 when not over the cap
    weekly_summary['payout_adjustment'] = 1 - (weekly_summary['amount_over'] / weekly_summary['total_payout']).clip(upper=1)

    # Ensuring the adjustment doesn't go above 1 when there's no need to adjust
    weekly_summary['payout_adjustment'] = weekly_summary['payout_adjustment'].clip(lower=0, upper=1)

    # Rename columns 
    weekly_summary.rename(columns={'year': 'Year', 'week': 'Week Number'}, inplace=True)

    # Create the 'Payouts' DataFrame
    # Group by character and week, and sum up the number of connections
    character_weekly = df.groupby(['character_name', 'year', 'week']).agg(
        connections_sum=('connectionCreate', 'sum')
    ).reset_index()

    # Merge with weekly_summary to get the week ending and payout_adjustment
    character_payouts = pd.merge(
        character_weekly, 
        weekly_summary[['Year', 'Week Number', 'Week Ending', 'payout_adjustment']], 
        left_on=['year', 'week'], 
        right_on=['Year', 'Week Number']
    )

    # Calculate the actual payout for each character per week
    character_payouts['actual_payout'] = character_payouts['connections_sum'] * payout_per_connection * character_payouts['payout_adjustment']

    # Select and rename columns
    character_payouts = character_payouts[['character_name', 'connections_sum', 'Week Ending', 'payout_adjustment', 'actual_payout']]
    
    # Exporting to Excel with 'All Data' as the sheet name
    print("Exporting All Data")
    
    with pd.ExcelWriter(excel_file_name, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='All Data', index=False)
        weekly_summary.to_excel(writer, sheet_name='Weekly Summary', index=False)
        character_payouts.to_excel(writer, sheet_name='Payouts', index=False)
    
    print(f"Data exported to {excel_file_name}.")

    # Close cursor and connection
    cursor.close()
    db_connection.close()

if __name__=="__main__":
    main()