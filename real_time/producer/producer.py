from concurrent.futures import ThreadPoolExecutor
from confluent_kafka import Producer
import requests
import json
import time


# Kafka broker configuration
kafka_config = {
    'bootstrap.servers': "localhost:9092"
}

# Kafka topics
cars_topic = 'cars_sales_topic'

# Flask API URLs
cars_api_url = 'http://localhost:5000/cars/sales'

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
        print('Message content: {}'.format(msg.value().decode('utf-8')))

def fetch_data_with_pagination(api_url, kafka_producer, kafka_topic):
    page = 1
    per_page = 100  # Adjust per_page as needed

    while True:
        try:
            # Fetch data from the API with pagination parameters
            response = requests.get(api_url, params={'page': page, 'per_page': per_page})
            data = response.json()


            # Produce data to Kafka topic with the current date
            for record in data['cars']:
                record_value = json.dumps(record)
                kafka_producer.produce(kafka_topic, key=None, value=record_value, callback=delivery_report)
                kafka_producer.flush()  # Flush after each message

            # Check if there are more pages
            if len(data['cars']) < per_page:
                break  # No more pages

            # Increment page number for the next request
            page += 1

            # Introduce a delay if needed to avoid overloading the API
            time.sleep(1)

        except Exception as e:
            print(f"Error fetching or producing data: {e}")
            break

if __name__ == '__main__':
    # Create Kafka producer instance
    producer = Producer(kafka_config)

    # Use ThreadPoolExecutor to parallelize data transfer
    with ThreadPoolExecutor() as executor:
        # Schedule the execution of fetch_data_with_pagination
        futures = [
             executor.submit(fetch_data_with_pagination, cars_api_url, producer, cars_topic)
        ]

        # Wait for all futures to complete
        for future in futures:
            future.result()

    # Sleep for a while to allow the producer to finish delivering messages
    time.sleep(2)
