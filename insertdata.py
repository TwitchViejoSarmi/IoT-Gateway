import os
import json
from pathlib import Path
from dotenv import load_dotenv
import boto3
import pg8000

# Load environment variables
load_dotenv()

def get_s3_session():
    """
    Returns an active AWS session.
    """
    session = boto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
        region_name=os.getenv("AWS_REGION")
    )
    return session

def get_sql_session():
    """
    Returns an active PostgreSQL connection and cursor.
    """
    conn = pg8000.connect(
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT"))
    )
    cur = conn.cursor()
    return conn, cur

def insert_data(data: dict, conn, cur):
    """
    Inserts the data into the `datalogs` table in PostgreSQL.
    """
    query = """
        INSERT INTO datalogs (sensor_id, timestamp, temperature, humidity, location, battery_level)
        VALUES (%s, %s, %s, %s, POINT(%s, %s), %s) ON CONFLICT (sensor_id, timestamp) DO NOTHING
    """
    inserts = (
        data["sensor_id"], data["timestamp"], data["temperature"],
        data["humidity"], data["location"]["latitude"],
        data["location"]["longitude"], data["battery_level"]
    )
    
    cur.execute(query, inserts)
    conn.commit()
    print(f"Inserted {data}")

def get_json_files(bucket_name: str, session):
    """
    Gets all JSON files from an S3 bucket and inserts them into the database.
    """
    s3 = session.client("s3")
    response = s3.list_objects_v2(Bucket=bucket_name)
    conn, cur = get_sql_session()

    if "Contents" in response:
        for obj in response["Contents"]:
            file = obj["Key"]
            file_data = s3.get_object(Bucket=bucket_name, Key=file)
            extension = Path(file).suffix.lower()

            if extension == ".json":
                content = file_data["Body"].read().decode("utf-8")
                data = json.loads(content)["measurements"]
                for register in data:
                    insert_data(register, conn, cur)
    else:
        print("No files detected...")

session = get_s3_session()
get_json_files("mys3wetahhbucket", session)
