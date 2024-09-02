import json
import pandas as pd
import boto3
import ast
from io import StringIO
import s3_file_operations as s3_ops

def lambda_handler(event, context):
    bucket = "de-masterclass-ogutu"  # S3 bucket name

    # Read data from S3
    print("Reading Character data from S3...")
    char_df = s3_ops.read_csv_from_s3(bucket, 'Rick&Morty/Untransformed/characters.csv')
    print(f"Characters DataFrame shape: {char_df.shape}")
    print("\nReading Episode data from S3...")
    ep_df = s3_ops.read_csv_from_s3(bucket, 'Rick&Morty/Untransformed/episodes.csv')
    print(f"Episodes DataFrame shape: {ep_df.shape}")
    print("\nReading Location data from S3...")
    loc_df = s3_ops.read_csv_from_s3(bucket, 'Rick&Morty/Untransformed/locations.csv')
    print(f"Locations DataFrame shape: {loc_df.shape}")

    # Check if data is loaded successfully
    if char_df is None or ep_df is None or loc_df is None:
        print("Error in loading data from S3")
        return {
            'statusCode': 500,
            'body': json.dumps('Error in loading data from S3')
        }

    print("Data loaded successfully from S3")

    # Characters DataFrame transformation
    extract_id = lambda x: x.split('/')[-1] if x else None

    char_df['origin_id'] = [
        extract_id(ast.literal_eval(record)['url']) if isinstance(record, str) else None
        for record in char_df['origin']
    ]

    char_df['location_id'] = [
        extract_id(ast.literal_eval(record)['url']) if isinstance(record, str) else None
        for record in char_df['location']
    ]

    char_df = char_df.drop(columns=['origin', 'location', 'episode'])

    # Appearance DataFrame Creation
    appearance_df = ep_df.copy()

    character_func = lambda x: [url.split('/')[-1] for url in ast.literal_eval(x)] if isinstance(x, str) else None

    appearance_df['character_ids'] = [
        character_func(record) if record else None
        for record in appearance_df['characters']
    ]

    expanded_df = appearance_df.explode('character_ids')
    expanded_df = expanded_df.reset_index(drop=True).reset_index().rename(columns={'index': 'id'})
    expanded_df = expanded_df.rename(columns={'id': 'id', 'id': 'episode_id', 'character_ids': 'character_id'})
    expanded_df = expanded_df[['id', 'episode_id', 'character_id']]

    # Episodes DataFrame transformation
    ep_df = ep_df.drop("characters", axis=1)

    # Locations DataFrame transformation
    loc_df = loc_df.drop('residents', axis=1)

    # Save final DataFrames to S3
    s3_ops.write_data_to_s3(char_df, bucket, 'Rick&Morty/Transformed/Character.csv')
    s3_ops.write_data_to_s3(ep_df, bucket, 'Rick&Morty/Transformed/Episode.csv')
    s3_ops.write_data_to_s3(expanded_df, bucket, 'Rick&Morty/Transformed/Appearance.csv')
    s3_ops.write_data_to_s3(loc_df, bucket, 'Rick&Morty/Transformed/Location.csv')

    return {
        'statusCode': 200,
        'body': json.dumps('Data transformation and upload successful')
    }