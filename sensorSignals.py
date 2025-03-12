import json
import random
import time
import boto3
from datetime import datetime, timedelta

BUCKET_NAME = "mys3wetahhbucket"
s3_client = boto3.client('s3')

sensors = [
    {
        "sensor_id": "THS-001",
        "latitude": 37.7749,
        "longitude": -122.4194
    },
    {
        "sensor_id": "THS-002",
        "latitude": 37.7750,
        "longitude": -122.4195
    },
    {
        "sensor_id": "THS-003",
        "latitude": 37.7751,
        "longitude": -122.4196
    },
]


# Función de mediciones aleatorias
def generateMeasure(sensor_id, latitude, longitude, timestamp):
    return {
        "sensor_id": sensor_id,
        "timestamp": timestamp.isoformat() + "Z",
        "temperature": round(random.uniform(20, 30), 1),
        "humidity": round(random.uniform(50, 70), 1),
        "location": {
            "latitude": latitude,
            "longitude": longitude
        },
        "battery_level": random.randint(80, 100)
    }


# Función para generar y subir archivos JSON a S3
def generateUploadBoton(num_files=5, num_measurements=12):
    for i in range(num_files):
        measurements = []
        start_time = datetime.utcnow()

        for _ in range(num_measurements):
            sensor = random.choice(sensors)
            timestamp = start_time + timedelta(seconds=random.randint(0, 60))
            measurements.append(
                generateMeasure(sensor["sensor_id"], sensor["latitude"],
                                sensor["longitude"], timestamp))

        json_data = {"measurements": measurements}
        file_name = f"sensor_data_{i+1}.json"

        with open(file_name, "w") as json_file:
            json.dump(json_data, json_file, indent=4)

        try:
            response = s3_client.upload_file(file_name, BUCKET_NAME, file_name)
            print(f"response: {response}")
            print(f"File '{file_name}' uploaded to '{BUCKET_NAME}/{file_name}'")
        except Exception as e:
            print(f"Error uploading file: {e}")
        time.sleep(10)


generateUploadBoton()
