import pandas as pd
import requests
import json
import datetime
import s3_file_operations as s3_ops

def lambda_handler(event, context):
    # TODO implement
    endpoints = ["character", "episode", "location"]
    
    data_dict = {
        "character": [],
        "episode": [],
        "location": []
        }
    for endpoint in endpoints:
        print(f"\nExtracting data from {endpoint} endpoint.")
    
        page = 1
    
        next = True
    
        while next:
    
            print(f"Extracting page {page} data...")
    
            #Fetch data from the current page of the endpoint
            r = requests.get(f"https://rickandmortyapi.com/api/{endpoint}/?page={str(page)}")
            data = r.json().get("results", [])
    
            # Add the data to the corresponding list in the dictionary
            data_dict[endpoint].extend(data)
    
            #Checking if the value of the next url is a null or None, so we stop the loop
            if r.json().get("info", {}).get("next") is not None:
                page += 1
            else:
                break
        
        print(f"\nFinished extracting data from {endpoint} endpoint. Total records: {len(data_dict[endpoint])}")
    
    # Converting the data lists to DataFrames
    char_df = pd.DataFrame(data_dict["character"])
    loc_df = pd.DataFrame(data_dict["location"])
    ep_df = pd.DataFrame(data_dict["episode"])
    
    print("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    print(f"\nAll data fully extracted.\nCharacter structure: {char_df.shape},\nLocation structure: {loc_df.shape},\nEpisode structure: {ep_df.shape}")
    
    #Saving the data to s3
    # Dictionary mapping DataFrame names to their corresponding keys in S3
    dataframes = {
        "characters.csv": char_df,
        "locations.csv": loc_df,
        "episodes.csv": ep_df
    }
    
    # S3 bucket name
    bucket = "de-masterclass-ogutu"
    
    # Looping through each DataFrame and uploading it to S3
    for filename, dataframe in dataframes.items():
        key = f"Rick&Morty/Untransformed/{filename}"
        s3_ops.write_data_to_s3(dataframe, bucket_name=bucket, key=key)
        print(f"{filename} uploaded successfully to {key}!")
        
    print("\nData successfuly saved in s3, you can go check it out!")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
