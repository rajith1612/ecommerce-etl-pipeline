import requests
import boto3
import json
import os
import time

# AWS and API Config
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
STREAM_NAME = os.getenv("KINESIS_STREAM_NAME", "ecommerce-stream")
API_URL = "https://fakestoreapi.com/products"

# Initialize Kinesis client
kinesis = boto3.client("kinesis", region_name=AWS_REGION)

def fetch_and_send():
    while True:
        try:
            response = requests.get(API_URL)
            if response.status_code == 200:
                data = response.json()
                for item in data:
                    json_data = json.dumps(item)
                    print(f"🚀 Sending to Kinesis: {json_data}")
                    response = kinesis.put_record(
                        StreamName=STREAM_NAME,
                        Data=json_data,
                        PartitionKey=str(item.get("id", "1"))
                    )
                    print("✅ Kinesis write response:", response)
            else:
                print("❌ API Error:", response.status_code, response.text)
        except Exception as e:
            print("❗ Error:", e)

        time.sleep(10)  # Fetch every 10 seconds

if __name__ == "__main__":
    fetch_and_send()
