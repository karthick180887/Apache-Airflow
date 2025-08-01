import requests
import boto3
import time
import json
from datetime import datetime

# Configuration
OPENWEATHER_API_KEY = 'ac0f678863b6491892a319c814f8df59'  # Replace with your API key
CITY_NAME = 'London'  # Change to your desired city
COUNTRY_CODE = 'UK'  # Change to your country code
KINESIS_STREAM_NAME = 'project'
AWS_REGION = 'ap-south-1'

# Initialize AWS Kinesis client
kinesis_client = boto3.client('kinesis', region_name=AWS_REGION)

def get_weather_data():
    """Fetch current weather data from OpenWeather API"""
    base_url = "http://api.openweathermap.org/data/2.5/weather"
    params = {
        'q': f'{CITY_NAME},{COUNTRY_CODE}',
        'appid': OPENWEATHER_API_KEY,
        'units': 'metric'  # Get temperature in Celsius
    }
    
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching weather data: {e}")
        return None

def push_to_kinesis(data):
    """Push data to AWS Kinesis stream"""
    try:
        response = kinesis_client.put_record(
            StreamName=KINESIS_STREAM_NAME,
            Data=json.dumps(data),
            PartitionKey=str(data.get('dt', int(time.time())))  # Using timestamp as partition key
        )
        print(f"Data pushed to Kinesis. SequenceNumber: {response['SequenceNumber']}")
    except Exception as e:
        print(f"Error pushing to Kinesis: {e}")

def main():
    print("Starting weather data streaming to AWS Kinesis...")
    
    while True:
        # Get current timestamp
        current_time = datetime.utcnow().isoformat()
        
        # Fetch weather data
        weather_data = get_weather_data()
        
        if weather_data:
            # Add timestamp to the data
            weather_data['ingestion_timestamp'] = current_time
            
            # Print and push data
            print(f"Fetched weather data at {current_time}:")
            print(json.dumps(weather_data, indent=2))
            
            push_to_kinesis(weather_data)
        
        # Wait for 1 second before next iteration
        time.sleep(1)

if __name__ == "__main__":
    main()