import json
import pandas as pd
import pymysql.cursors
import s3_file_operations as s3_ops

rds_host = "database-1.ct0yog8ymd5e.eu-west-1.rds.amazonaws.com" # Replace with your RDS hostname
rds_username = "admin"
rds_user_pwd = "joyvic8085."  # Replace with your password
rds_db_name = "rick-and-morty-db"
bucket_name = "de-masterclass-ogutu"   # Replace with your s3 Bucket name

def lambda_handler(event, context):
    # Read transformed data from S3
    print("Reading transformed data from S3...")
    char_df = s3_ops.read_csv_from_s3(bucket_name, 'Rick&Morty/Transformed/characters.csv')
    ep_df = s3_ops.read_csv_from_s3(bucket_name, 'Rick&Morty/Transformed/episodes.csv')
    app_df = s3_ops.read_csv_from_s3(bucket_name, 'Rick&Morty/Transformed/appearances.csv')
    loc_df = s3_ops.read_csv_from_s3(bucket_name, 'Rick&Morty/Transformed/locations.csv')

    # Check if data is loaded successfully
    if char_df is None or ep_df is None or app_df is None or loc_df is None:
        print("Error in loading data from S3")
        return {
            'statusCode': 500,
            'body': json.dumps('Error in loading data from S3')
        }

    print("Data loaded successfully from S3")

    # SQL create table scripts
    create_character_table = """
        CREATE TABLE IF NOT EXISTS Character_Table (
            id INT NOT NULL PRIMARY KEY,
            name VARCHAR(255),
            status VARCHAR(255),
            species VARCHAR(255),
            type VARCHAR(255),
            gender VARCHAR(255),
            origin_id VARCHAR(255),
            location_id VARCHAR(255),
            image VARCHAR(255),
            url VARCHAR(255),
            created TIMESTAMP
        ) ENGINE=INNODB;
    """

    create_episode_table = """
        CREATE TABLE IF NOT EXISTS Episode_Table (
            id INT NOT NULL PRIMARY KEY,
            name VARCHAR(255),
            air_date VARCHAR(255),
            episode VARCHAR(255),
            url VARCHAR(255),
            created TIMESTAMP
        ) ENGINE=INNODB;
    """

    create_appearance_table = """
        CREATE TABLE IF NOT EXISTS Appearance_Table (
            id INT NOT NULL PRIMARY KEY,
            episode_id INT,
            character_id INT
        ) ENGINE=INNODB;
    """

    create_location_table = """
        CREATE TABLE IF NOT EXISTS Location_Table (
            id INT NOT NULL PRIMARY KEY,
            name VARCHAR(255),
            type VARCHAR(255),
            dimension VARCHAR(255),
            url VARCHAR(255),
            created TIMESTAMP
        ) ENGINE=INNODB;
    """

    try:
        conn = pymysql.connect(host=rds_host,
                               user=rds_username,
                               password=rds_user_pwd,
                               port=3306,
                               database=rds_db_name,
                               cursorclass=pymysql.cursors.DictCursor)
        cursor = conn.cursor()

        # Create tables
        cursor.execute(create_character_table)
        cursor.execute(create_episode_table)
        cursor.execute(create_appearance_table)
        cursor.execute(create_location_table)

        # Insert data into Character_Table
        insert_data(cursor, conn, char_df, "Character_Table")

        # Insert data into Episode_Table
        insert_data(cursor, conn, ep_df, "Episode_Table")

        # Insert data into Appearance_Table
        insert_data(cursor, conn, app_df, "Appearance_Table")

        # Insert data into Location_Table
        insert_data(cursor, conn, loc_df, "Location_Table")

        print("Data insertion completed successfully")

    except Exception as e:
        print("Exception: ", e)
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }

    return {
        'statusCode': 200,
        'body': json.dumps('Data transformation and upload successful')
    }

def insert_data(cursor, conn, df, table_name):
    column_names = list(df.columns)
    for i, row in df.iterrows():
        placeholders = ','.join(['%s'] * len(column_names))
        sql_insert = f"INSERT INTO {table_name} ({','.join(column_names)}) VALUES ({placeholders});"
        data = tuple(row[column] for column in column_names)
        cursor.execute(sql_insert, data)
        conn.commit()