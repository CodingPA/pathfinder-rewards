import mysql.connector

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

try:
    # Establishing the connection to the database
    cnx = mysql.connector.connect(**config)
    print("Successfully connected to the database!")

    # You can now use cnx to interact with the database
    cursor = cnx.cursor()
    cursor.execute("SHOW TABLES;")
    for table in cursor:
        print(table)

    # close the connection when done
    cnx.close()

except mysql.connector.Error as err:
    print(f"Failed to connect: {err}")

# Expand this script to interact with the database, perform queries, and handle results.
