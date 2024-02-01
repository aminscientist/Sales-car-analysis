import pyodbc
import pandas as pd

def connect_with_sql_server_1():
    # SQL Server connection parameters
    server = '192.168.102.1'
    database = 'StagingArea'
    username = 'aminscientist'
    password = '2021'

    # Connection string :
    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

    # Create a connection
    try:
        conn1 = pyodbc.connect(connection_string)
        print("Connection successful!")
        return conn1
    except Exception as e:
        print(f"Connection failed. Error: {e}")

conn1 = connect_with_sql_server_1()

# Check if the connection is successful before proceeding
if conn1:
    # SQL query to fetch data from the database
    sql_query = 'SELECT * FROM CarsSalesTransformed'

    # Read data into a DataFrame
    df = pd.read_sql_query(sql_query, conn1)

conn1.close()
def connect_with_sql_server():
    # SQL Server connection parameters
    server = '192.168.102.1'
    database = 'AutoCarsAnalyticsDW'
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

cursor.execute('''
    CREATE TABLE DimCar (
        CarID INT PRIMARY KEY,
        CarMake VARCHAR(255),
        CarModel VARCHAR(255),
        OriginCountry VARCHAR(255),
        Fuel VARCHAR(255),
        VehicleType VARCHAR(255),
        Transmission VARCHAR(255)
    )
''')

# Create DimCustomer table
cursor.execute('''
    CREATE TABLE DimCustomer (
        CustomerID INT PRIMARY KEY,
        CustomerName VARCHAR(255),
        Country VARCHAR(255)
    )
''')

# Create DimDate table
cursor.execute('''
    CREATE TABLE DimDate (
        DateID INT PRIMARY KEY,
        Date DATETIME,
        CarYear INT
    )
''')

# Create DimSalesperson table
cursor.execute('''
    CREATE TABLE DimSalesperson (
        SalespersonID INT PRIMARY KEY,
        SalespersonName VARCHAR(255)
    )
''')

# Create DimVehicle table
# cursor.execute('''
#     CREATE TABLE DimVehicle (
#         VehicleID INT PRIMARY KEY,
#         Fuel VARCHAR(255),
#         VehicleType VARCHAR(255),
#         Transmission VARCHAR(255)
#     )
# ''')

conn.commit()

# Create FactSales table
cursor.execute('''
    CREATE TABLE FactSales (
        SaleID INT PRIMARY KEY IDENTITY(1,1),
        CarID INT,
        CustomerID INT,
        DateID INT,
        SalespersonID INT,
        CommissionEarned FLOAT,
        CommissionRate FLOAT,
        SalePrice INT,
        KilometersDriven INT,
        EngineSize FLOAT,
        Horsepower INT,
        Cylinders INT,
        Weight FLOAT,
        Speed FLOAT,
        Doors INT,
        EPAFuelEconomyScore INT,
        FuelEfficiency FLOAT,
        SafetyRatings INT,
        CONSTRAINT FK_Car FOREIGN KEY (CarID) REFERENCES DimCar(CarID),
        CONSTRAINT FK_Customer FOREIGN KEY (CustomerID) REFERENCES DimCustomer(CustomerID),
        CONSTRAINT FK_Date FOREIGN KEY (DateID) REFERENCES DimDate(DateID),
        CONSTRAINT FK_Salesperson FOREIGN KEY (SalespersonID) REFERENCES DimSalesperson(SalespersonID),
    )
''')

# Commit the changes
conn.commit()

df_dim_car = df[['car_make', 'car_model', 'origin_country', 'fuel', 'vehicle_type', 'transmission']].drop_duplicates()
df_dim_car['car_id'] = pd.RangeIndex(start=1, stop=len(df_dim_car) + 1)

for index, row in df_dim_car.iterrows():
    car_id = row['car_id']
    car_make = row['car_make']
    car_model = row['car_model']
    origin_country = row['origin_country']
    fuel = row['fuel']
    vehicle_type = row['vehicle_type']
    transmission = row['transmission']

    # Execute the INSERT query with parameters
    cursor.execute('''
        INSERT INTO DimCar (CarID, CarMake, CarModel, OriginCountry, Fuel, VehicleType, Transmission)
        VALUES (?, ?, ?, ?,?,?,?)
    ''', car_id, car_make, car_model, origin_country, fuel, vehicle_type, transmission)

# Commit the changes and close the connection
conn.commit()

df_dim_customer = df[['customer_name', 'country']]
df_dim_customer = df_dim_customer.drop_duplicates(subset=['customer_name', 'country'])
df_dim_customer['customer_id'] = pd.RangeIndex(start=1, stop=len(df_dim_customer) + 1)

for index, row in df_dim_customer.iterrows():
    customer_id = row['customer_id']
    customer_name = row['customer_name']
    country = row['country']

    # Execute the INSERT query with parameters
    cursor.execute('''
        INSERT INTO DimCustomer (CustomerID, CustomerName, Country)
        VALUES (?, ?, ?)
    ''', customer_id, customer_name, country)

# Commit the changes and close the connection
conn.commit()

df_dim_date = df[['date', 'car_year']].drop_duplicates().reset_index(drop=True)
df_dim_date['date_id'] = pd.RangeIndex(start=1, stop=len(df_dim_date) + 1)

# Iterate over rows in the df_dim_date DataFrame and insert data into the DimDate table
for index, row in df_dim_date.iterrows():
    date_id = row['date_id']
    date_value = row['date']
    car_year = row['car_year']

    # Execute the INSERT query with parameters
    cursor.execute('''
        INSERT INTO DimDate (DateID, Date, CarYear)
        VALUES (?, ?, ?)
    ''', date_id, date_value, car_year)

# Commit the changes and close the connection
conn.commit()

df_dim_salesperson = df[['salesperson']].drop_duplicates().reset_index(drop=True)
df_dim_salesperson['salesperson_id'] = pd.RangeIndex(start=1, stop=len(df_dim_salesperson) + 1)

for index, row in df_dim_salesperson.iterrows():
    salesperson_id = row['salesperson_id']
    salesperson_name = row['salesperson']

    # Execute the INSERT query with parameters
    cursor.execute('''
        INSERT INTO DimSalesperson (SalespersonID, SalespersonName)
        VALUES (?, ?)
    ''', salesperson_id, salesperson_name)

# Commit the changes and close the connection
conn.commit()

# df_dim_vehicle = df[['fuel', 'vehicle_type', 'transmission']].drop_duplicates().reset_index(drop=True)
# df_dim_vehicle['vehicle_id'] = pd.RangeIndex(start=1, stop=len(df_dim_vehicle) + 1)
#
# # Iterate over the rows in df_dim_vehicle
# for index, row in df_dim_vehicle.iterrows():
#     vehicle_id = row['vehicle_id']
#     fuel = row['fuel']
#     vehicle_type = row['vehicle_type']
#     transmission = row['transmission']
#
#     # Execute the INSERT query with parameters
#     cursor.execute('''
#         INSERT INTO DimVehicle (VehicleID, Fuel, VehicleType, Transmission)
#         VALUES (?, ?, ?, ?)
#     ''', vehicle_id, fuel, vehicle_type, transmission)
#
# # Commit the changes and close the connection
# conn.commit()

# Replace Car details with CarID from DimCar
df = df.merge(df_dim_car, on=['car_make', 'car_model', 'origin_country', 'fuel', 'vehicle_type', 'transmission'], how='left')
df.drop(['car_make', 'car_model', 'origin_country', 'fuel', 'vehicle_type', 'transmission'], axis=1, inplace=True)

# Replace Customer details with CustomerID from DimCustomer
df = df.merge(df_dim_customer, on=['customer_name', 'country'], how='left')
df.drop(['customer_name', 'country'], axis=1, inplace=True)

# Replace Date details with DateID from DimDate
df = df.merge(df_dim_date, on=['car_year', 'date'], how='left')
df.drop(['car_year', 'date'], axis=1, inplace=True)

# Replace Salesperson details with SalespersonID from DimSalesperson
df = df.merge(df_dim_salesperson, on=['salesperson'], how='left')
df.drop(['salesperson'], axis=1, inplace=True)

# Replace Vehicle details with VehicleID from DimVehicle
# df = df.merge(df_dim_vehicle, on=['fuel', 'vehicle_type', 'transmission'], how='left')
# df.drop(['fuel', 'vehicle_type', 'transmission'], axis=1, inplace=True)

for index, row in df.iterrows():
    car_id = row['car_id']
    customer_id = row['customer_id']
    date_id = row['date_id']
    salesperson_id = row['salesperson_id']
    commission_earned = row['commission_earned']
    commission_rate = row['commission_rate']
    sale_price = row['sale_price']
    kilometers_driven = row['kilometers_driven']
    engine_size = row['engine_size']
    horsepower = row['horsepower']
    cylinders = row['cylinders']
    weight = row['weight']
    speed = row['speed']
    doors = row['doors']
    epa_fuel_economy_score = row['epa_fuel_economy_score']
    fuel_efficiency = row['fuel_efficiency']
    safety_ratings = row['safety_ratings']

    # Execute the INSERT query with parameters into your FactSales table
    cursor.execute('''
        INSERT INTO FactSales (
            CarID, CustomerID,DateID, SalespersonID, CommissionEarned, CommissionRate, SalePrice,
            KilometersDriven, EngineSize, Horsepower, Cylinders,
            Weight, Speed, Doors, EpaFuelEconomyScore,
            FuelEfficiency, SafetyRatings
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', car_id, customer_id, date_id, salesperson_id,
                   commission_earned, commission_rate, sale_price,
                   kilometers_driven, engine_size, horsepower, cylinders,
                   weight, speed, doors, epa_fuel_economy_score,
                   fuel_efficiency, safety_ratings)

# Commit the changes and close the connection
conn.commit()

conn.close()

print('data loading done!')