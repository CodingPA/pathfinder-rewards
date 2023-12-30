import mysql.connector
import sys

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

def main():
    print("Starting Script")

    creds = get_credentials()

    db_connection = connect_to_db(creds)

    cursor = db_connection.cursor()

    #Do things here

    # Close cursor and connection
    cursor.close()
    db_connection.close()

if __name__=="__main__":
    main()