import pandas as pd
import requests
from hdfs import InsecureClient
import pyarrow as pa

df_cars_infos = pd.read_csv(r'../external_data/infos_cars_f.csv')

# API endpoint URL
api_url = 'http://127.0.0.1:5000/cars/sales'

# Function to get data from the API with pagination
def get_data_by_page(page):
    response = requests.get(api_url, params={'page': page})
    if response.status_code == 200:
        data = response.json()
        return data['cars']
    else:
        print(f"Error: {response.status_code}")
        return None

# Function to get all data from the API
def get_all_data(num_pages):
    all_data = []  # List to store data from all pages

    for page in range(1, num_pages + 1):
        # Get data from the current page
        page_data = get_data_by_page(page)

        # Check if there is data on the current page
        if page_data:
            # Append data from the current page to the list
            all_data.extend(page_data)
        else:
            break  # Break the loop if there is no more data

    # Convert the list of data to a DataFrame
    df_all_data = pd.DataFrame(all_data)

    return df_all_data

# Specify the number of pages you want to fetch
num_pages_to_fetch = 800

# Call the function to get data from the specified number of pages
df_all_data = get_all_data(num_pages_to_fetch)

merged_df = pd.merge(df_all_data, df_cars_infos, left_on=['Car Make', 'Car Model'], right_on=['Make', 'Model'], how='left')

merged_df.drop(columns=['Make', 'Model'], inplace=True)

# merged_df.to_parquet(r'..\external_data\dataset.parquet', engine='pyarrow')

client = InsecureClient("http://localhost:9870", user="amine")

with client.write("/cars/dataset.csv", overwrite=True) as writer:
    merged_df.to_csv(writer)

print('data extraction done')