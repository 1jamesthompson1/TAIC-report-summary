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

    report_groups = [v.reset_index(drop=True) for k, v in df.groupby("report_id")]

    widened_df = pd.DataFrame(
        {
            "report_id": df.groupby("report_id").groups.keys(),
            "recommendations": report_groups,
        }
    )

    widened_df.to_pickle(output_file_name)
