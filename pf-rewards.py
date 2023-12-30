import mysql.connector
import sys
import pandas as pd

def get_credentials():
    # Read the credentials and IP from the file
    with open('cred.txt', 'r') as file:
        username = file.readline().strip()  # Read the first line for username
        password = file.readline().strip()  # Read the second line for password
        db_ip = file.readline().strip()     # Read the third line for server IP

    # Connection configuration
    config = {
        'user': username,
        'password': password,
        'host': db_ip,  
        'port': 3306,
        'database': 'mkuga_pathfinder',
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
        SELECT *
        FROM activity_log
        LEFT JOIN `character` ON activity_log.characterId = `character`.id
        LEFT JOIN alliance ON character.allianceId = alliance.id;
    """

    df = fetch_data_to_dataframe(cursor, query)
    
    # Exporting to Excel with 'All Data' as the sheet name
    print("Exporting All Data")
    df.to_excel(excel_file_name, sheet_name='All Data', index=False)
    print(f"Data exported to {excel_file_name} in the sheet named 'All Data'.")



    # Close cursor and connection
    cursor.close()
    db_connection.close()

if __name__=="__main__":
    main()