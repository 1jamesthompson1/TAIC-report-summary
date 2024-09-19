import os

import pandas as pd
import requests


class DataGetter:
    def __init__(self, local_data_location, remote_data_location, refresh):
        self.local_data_location = local_data_location
        self.remote_data_location = remote_data_location
        self.refresh = refresh

    def get_data_path(self, data_name):
        """
        Generic function that will get data from either the remote or local location.
        Assumes all data files are csv pandas dataframes
        """

        local_path = os.path.join(self.local_data_location, data_name)

        if os.path.exists(local_path):
            return local_path

        remote_path = os.path.join(self.remote_data_location, data_name)

        response = requests.get(remote_path)

        if response.status_code == 200:
            return remote_path

        if response.status_code == 404:
            raise FileNotFoundError(
                f"Could not find {data_name} on the internet ({remote_path}) or locally at {local_path}"
            )

    def get_recommendations(self, data_name, output_file_name):
        if os.path.exists(output_file_name) and not self.refresh:
            return

        data_path = self.get_data_path(data_name)

        df = pd.read_csv(data_path)

        report_groups = [v.reset_index(drop=True) for k, v in df.groupby("report_id")]

        widened_df = pd.DataFrame(
            {
                "report_id": df.groupby("report_id").groups.keys(),
                "recommendations": report_groups,
            }
        )

        widened_df.to_pickle(output_file_name)

    def get_generic_data(self, data_location, output_file_name):
        """
        Gets the data from a datasource and stores it in the output file location.
        The output location is expected to be in the output folder
        """

        if os.path.exists(output_file_name) and not self.refresh:
            return

        data_path = self.get_data_path(data_location)

        df = pd.read_csv(data_path)

        df.to_pickle(output_file_name)
