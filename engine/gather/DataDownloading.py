import pandas as pd
import os


def get_recommendations(data_location, output_file_name, refresh):
    """
    This function will collect data from the data location. 
    It assumes that the recommendation file is a csv.
    """

    print("Getting recommendation file from the data folder...")

    if os.path.exists(output_file_name) and not refresh:
        return
    
    print(f"  {data_location} does not exist, trying to get from GitHub...")

    df = pd.read_csv(data_location)

    print(df)

    df.to_pickle(output_file_name)
