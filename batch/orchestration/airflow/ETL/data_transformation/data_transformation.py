import pandas as pd
import re
import pyodbc
from hdfs import InsecureClient

client = InsecureClient("http://localhost:9870")

with client.read("/cars/dataset.csv") as reader:
    df = pd.read_csv(reader)

#df = pd.read_parquet(r'..\external_data\dataset.parquet')

# Rename columns to snake_case and remove whitespaces in-place
df.columns = df.columns.str.lower().str.replace(' ', '_')

print(df)

# Convert 'date' column to datetime
df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y')

# Columns to extract numeric values
numeric_columns = ['engine_size', 'engine_power', 'weight', 'speed', 'fuel_efficiency']


# Define a function to extract numeric values using regular expressions
def extract_numeric(s):
    # Use regular expression to extract numeric values
    numeric_values = re.findall(r'\d+\.?\d*', str(s))

    # Convert the list of numeric values to a single string and return
    return ' '.join(numeric_values)


# Apply the function to the specified columns
for column in numeric_columns:
    df[column] = df[column].apply(extract_numeric).astype(float)

# Remove '/5' from the 'safety_ratings' column
df['safety_ratings'] = df['safety_ratings'].str.replace('/5', '')

# Convert the 'safety_ratings' column to numeric type
df['safety_ratings'] = pd.to_numeric(df['safety_ratings'], errors='coerce')

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

# Créer un curseur pour exécuter les requêtes
cursor = conn.cursor()

cursor.execute('DROP TABLE IF EXISTS CarsSalesTransformed')

#créer la table Insurance
cursor.execute(
    ''' 
      CREATE TABLE CarsSalesTransformed (
        car_make VARCHAR(255),
        car_model VARCHAR(255),
        date DATETIME,
        car_year INT,
        commission_earned FLOAT,
        commission_rate FLOAT,
        country VARCHAR(255),
        customer_name VARCHAR(255),
        sale_price INT,
        salesperson VARCHAR(255),
        fuel VARCHAR(255),
        vehicle_type VARCHAR(255),
        engine_size FLOAT,
        horsepower INT,
        origin_country VARCHAR(255),
        transmission VARCHAR(255),
        kilometers_driven INT,
        engine_power FLOAT,
        cylinders INT,
        weight FLOAT,
        speed FLOAT,
        doors INT,
        epa_fuel_economy_score INT,
        fuel_efficiency FLOAT,
        safety_ratings INT
        )
    '''
              )

# Parcourir chaque ligne de la dataframe "df"
for index, row in df.iterrows():
    car_make = row['car_make']
    car_model = row['car_model']
    date = row['date']
    car_year = row['car_year']
    commission_earned = row['commission_earned']
    commission_rate = row['commission_rate']
    country = row['country']
    customer_name = row['customer_name']
    sale_price = row['sale_price']
    salesperson = row['salesperson']
    fuel = row['fuel']
    vehicle_type = row['vehicle_type']
    engine_size = row['engine_size']
    horsepower = row['horsepower']
    origin_country = row['origin_country']
    transmission = row['transmission']
    kilometers_driven = row['kilometers_driven']
    engine_power = row['engine_power']
    cylinders = row['cylinders']
    weight = row['weight']
    speed = row['speed']
    doors = row['doors']
    epa_fuel_economy_score = row['epa_fuel_economy_score']
    fuel_efficiency = row['fuel_efficiency']
    safety_ratings = row['safety_ratings']

    # Exécuter une requête INSERT pour insérer les valeurs dans la table "CarsSalesTables"
    cursor.execute("""
        INSERT INTO CarsSalesTransformed
        (car_make, car_model, date, car_year, commission_earned, commission_rate, country, customer_name, sale_price, salesperson,
         fuel, vehicle_type, engine_size, horsepower, origin_country, transmission, kilometers_driven, engine_power, cylinders, weight, speed, doors, epa_fuel_economy_score, fuel_efficiency, safety_ratings)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, car_make, car_model, date, car_year, commission_earned, commission_rate, country, customer_name, sale_price,
                   salesperson, fuel, vehicle_type, engine_size, horsepower, origin_country, transmission,
                   kilometers_driven,
                   engine_power, cylinders, weight, speed, doors, epa_fuel_economy_score, fuel_efficiency,
                   safety_ratings)

# Committer les modifications
conn.commit()

print('La table CarsSalesTransformed a été insérée avec succès')

conn.close()

print('data transformation done')