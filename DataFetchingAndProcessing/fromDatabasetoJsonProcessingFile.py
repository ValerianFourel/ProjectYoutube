from pyspark.sql import SparkSession
import json
import os
import argparse
from datetime import datetime

def create_spark_session():
    return SparkSession.builder \
        .appName("VideoDataProcessor") \
        .config("spark.driver.extraClassPath", "/home/vfourel/ProjectGym/postgresql-42.7.5.jar") \
        .getOrCreate()

def generate_unique_filename(base_name, extension, folder):
    filename = f"{base_name}{extension}"
    full_path = os.path.join(folder, filename)
    counter = 2

    while os.path.exists(full_path):
        filename = f"{base_name}_{counter}{extension}"
        full_path = os.path.join(folder, filename)
        counter += 1

    return full_path

def fetch_and_save_data(spark, database_config, output_folder, youtuber=None, search_terms=None):
    # Build the base query
    if youtuber:
        # If youtuber is specified, get all their videos regardless of search terms
        query = f"""
            SELECT * FROM videos 
            WHERE youtuber = '{youtuber}'
        """
    elif search_terms:
        # If only search terms are provided, search in all videos' titles
        search_conditions = " OR ".join([f"LOWER(title) LIKE LOWER('%{term}%')" for term in search_terms])
        query = f"""
            SELECT * FROM videos 
            WHERE {search_conditions}
        """
    else:
        # If neither youtuber nor search terms are provided, get all videos
        query = "SELECT * FROM videos"

    # Fetch data from database
    df = spark.read \
        .format("jdbc") \
        .option("url", database_config["url"]) \
        .option("driver", "org.postgresql.Driver") \
        .option("query", query) \
        .option("user", database_config["properties"]["user"]) \
        .option("password", database_config["properties"]["password"]) \
        .load()

    # Convert to dictionary format
    videos_dict = {
        "search_terms": search_terms if search_terms else "All videos processed",
        "videos": {}
    }

    # Convert DataFrame to dictionary
    for row in df.collect():
        row_dict = row.asDict()
        video_id = row_dict['id']

        # Format the video entry
        video_entry = {
            "upload_date": row_dict['upload_date'] or "NA",
            "duration": row_dict['duration'] or "NA",
            "view_count": row_dict['view_count'] or "NA",
            "like_count": row_dict['like_count'] or "NA",
            "title": row_dict['title'],
            "id": video_id,
            "webpage_url": row_dict['webpage_url'] or "NA",
            "description": row_dict['description'] or "NA",
            "subject_name": f"@{row_dict['youtuber']}"
        }
        videos_dict["videos"][video_id] = video_entry

    # Create output folder if it doesn't exist
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Generate output filename
    filename_parts = []
    if youtuber:
        filename_parts.append(youtuber)
    if search_terms:
        filename_parts.append("search")
    if not filename_parts:
        filename_parts.append("all")

    base_name = f"videos_{'_'.join(filename_parts)}_{datetime.now().strftime('%Y%m%d')}_matching_Download_ready"
    output_file = generate_unique_filename(base_name, ".json", output_folder)

    # Save to JSON file
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(videos_dict, f, indent=2)

    print(f"\nTotal videos in reduced set: {len(videos_dict['videos'])}")
    print(f"Created {output_file}")

def parse_arguments():
    parser = argparse.ArgumentParser(description='Fetch video data from database and save to JSON')
    parser.add_argument('--youtuber', 
                      help='Specific YouTuber to fetch (optional)',
                      default=None)
    parser.add_argument('--search-terms',
                      nargs='+',
                      help='Search terms to filter videos (optional)',
                      default=None)
    parser.add_argument('--output-folder', 
                      default='/home/vfourel/ProjectGym/ProjectGymArchive/ytdlpDataWarehouse',
                      help='Output folder for JSON files')
    parser.add_argument('--db-user', 
                      default='valerianfourel',
                      help='Database username')
    parser.add_argument('--db-password', 
                      default=os.environ.get('POSTGRES_PASSWORD'),
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
        # Fetch and save data
        fetch_and_save_data(spark, 
                          database_config, 
                          args.output_folder, 
                          args.youtuber, 
                          args.search_terms)
    finally:
        spark.stop()
