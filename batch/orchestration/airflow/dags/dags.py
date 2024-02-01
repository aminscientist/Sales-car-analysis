import pyodbc
def connect_with_sql_server():
    # SQL Server connection parameters
    server = '192.168.102.1'
    database = 'StagingArea'
    username = 'aminscientist'
    password = '2021'

    # Connection string :
    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

    # Create a connection
    try:
        conn = pyodbc.connect(connection_string)
        print("Connection successful!")
        return conn
    except Exception as e:
        print(f"Connection failed. Error: {e}")

conn = connect_with_sql_server()