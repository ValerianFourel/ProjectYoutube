from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import re
import sys
import os

def create_spark_session():
    return SparkSession.builder \
        .appName("VideoDataProcessor") \
        .config("spark.driver.extraClassPath", "/home/vfourel/ProjectGym/postgresql-42.7.5.jar") \
        .getOrCreate()

def parse_entry(entry, youtuber):  # Modified to accept youtuber parameter
    pattern = r'\|\s*(.*?)\s*\|\s*(.*?)\s*\|\s*(\d+|\w+)\s*\|\s*(.*?)\s*\|\s*(.*?)\s*\|\s*([\w-]+)\s*\|\s*(https?://\S+)\s*\|\s*(.*?)\s*\_/'
    match = re.match(pattern, entry, re.DOTALL)
    if not match:
        print(f"Failed to parse entry: {entry}...")
        return None

    return {
        "upload_date": match.group(1),
        "duration": match.group(2),
        "view_count": match.group(3),
        "like_count": match.group(4),
        "title": match.group(5),
        "id": match.group(6),
        "webpage_url": match.group(7),
        "description": match.group(8).strip(),
        "youtuber": youtuber  # Add youtuber to the returned dictionary
    }

def process_file(filename, youtuber):
    data_list = []

    with open(filename, 'r', encoding='utf-8') as file:
        content = file.read()
        print(f"File content length: {len(content)} characters")
        entries = content.split('\_/')
        print(f"Number of entries after split: {len(entries)}")

        for i, entry in enumerate(entries):
            entry = entry.strip()
            if entry:
                parsed_entry = parse_entry(entry + ' \_/', youtuber)
                if parsed_entry:
                    data_list.append(parsed_entry)
            else:
                print(f"Empty entry at index {i}")

    print(f"Processed entries: {len(data_list)}")
    return data_list


def save_to_database(spark, data_list, database_config):
    # Update schema to include youtuber field
    schema = StructType([
        StructField("upload_date", StringType(), True),
        StructField("duration", StringType(), True),
        StructField("view_count", StringType(), True),
        StructField("like_count", StringType(), True),
        StructField("title", StringType(), True),
        StructField("id", StringType(), False),
        StructField("webpage_url", StringType(), True),
        StructField("description", StringType(), True),
        StructField("youtuber", StringType(), True)  # Add this line
    ])

    df = spark.createDataFrame(data_list, schema)

    try:
        df.write \
            .format("jdbc") \
            .option("url", database_config["url"]) \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "videos") \
            .option("user", database_config["properties"]["user"]) \
            .option("password", database_config["properties"]["password"]) \
            .mode("append") \
            .save()
    except Exception as e:
        print(f"Error saving to database: {str(e)}")
        raise

import argparse

def parse_arguments():
    parser = argparse.ArgumentParser(description='Process YouTube video data and save to database')
    parser.add_argument('--input_file', 
                      help='Input file path containing video data')
    parser.add_argument('--youtuber', 
                      help='Name of the YouTuber (overrides filename parsing)',
                      default=None)
    parser.add_argument('--db-user', 
                      default='valerianfourel',
                      help='Database username')
    parser.add_argument('--db-password', 
                      default=os.environ.get('POSTGRES_PASSWORD'),  # Use environment variable
                      help='Database password')
    parser.add_argument('--db-host', 
                      default='localhost',
                      help='Database host')
    parser.add_argument('--db-port', 
                      default='5432',
                      help='Database port')
    parser.add_argument('--db-name', 
                      default='videos_db',
                      help='Database name')

    return parser.parse_args()

if __name__ == "__main__":
    # Parse command line arguments
    args = parse_arguments()

    # Extract youtuber name from filename if not provided
    if args.youtuber is None:
        filename = os.path.basename(args.input_file)
        youtuber_match = re.match(r'list_(.*?)_', filename)
        if youtuber_match:
            youtuber = youtuber_match.group(1)
        else:
            print("Error: YouTuber name not found in filename and not provided via --youtuber argument")
            sys.exit(1)
    else:
        youtuber = args.youtuber

    # Database configuration
    database_config = {
        "url": f"jdbc:postgresql://{args.db_host}:{args.db_port}/{args.db_name}",
        "properties": {
            "user": args.db_user,
            "password": args.db_password
        }
    }

    # Create Spark session
    spark = create_spark_session()

    try:
        # Process the file with youtuber information
        data_list = process_file(args.input_file, youtuber)

        # Save to database
        save_to_database(spark, data_list, database_config)

        print(f"Data has been successfully saved to the database for YouTuber: {youtuber}")

    finally:
        spark.stop()
