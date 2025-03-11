import os
import json
from pathlib import Path
from dotenv import load_dotenv
import boto3
import psycopg2

# Load enviornment.
load_dotenv()

def get_s3_session():
    """
        Function that returns the active session on the AWS CLI.
        Returns:
            (any): The actual session.
    """
    session = boto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION")
    )

    return session

def get_sql_session():
    """
        Function that returns an active cursos on PostgreSQL.
        Returns:
            (any): The actual cursos.
    """
    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )

    cur = conn.cursor()

    return conn, cur

def insert_data(data:dict, conn, cur):
    """
        Function that insert the data in the table datalogs in PostgreSQL.
        Params:
            data (dict): The data to insert.
            conn: The connection on PostgreSQL.
            cur: The cursor of PostgreSQL.
    """
    query = "INSERT INTO datalogs (sensor_id, timestamp, temperature, humidity, location, battery_level) VALUES (%s,%s,%s,%s,POINT(%s,%s),%s)"
    inserts = (data["sensor_id"], data["timestamp"], data["temperature"], data["humidity"], data["location"]["latitude"], data["location"]["longitude"], data["battery_level"])
    # Execute the query.
    cur.execute(query, inserts)

    # Commit the changes.
    conn.commit()

def get_json_files(bucket_name:str, session):
    """
        Gets all the json files in the Bucket S3.
        Params:
            bucket_name (str): The name of the bucket S3.
            session (any): The active session on AWS CLI.
    """
    s3 = session.client("s3")
    response = s3.list_objects_v2(Bucket=bucket_name) # Get the files in the bucket.
    conn, cur = get_sql_session()

    # Verify if there are files.
    if "Contents" in response:
        # Get each file.
        for obj in response["Contents"]:
            file = obj["Key"]
            file_data = obj["Body"]
            extension = Path(file).suffix.lower()

            if extension == ".json":

                # Create the folder if doesn't exists.
                os.makedirs("outputs",exist_ok=True)

                # Get the file content and convert it to dictionary.
                content = file_data.read().decode("utf-8")
                data = json.loads(content)
                insert_data(data, conn, cur)
    else:
        print("No files detected...")

