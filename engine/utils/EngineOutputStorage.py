import os
from datetime import datetime

import pytz
from azure.storage.blob import BlobServiceClient
from tqdm import tqdm


class EngineOutputManager:
    def __init__(self, storage_account_name, storage_account_key, container_name):
        self.engine_output_container = self.__get_output_container(
            storage_account_name, storage_account_key, container_name
        )

        self.output_date_format = "%Y-%m-%d_%H:%M:%S"

    def __get_output_container(
        self, storage_account_name, storage_account_key, container_name
    ):
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={storage_account_key}==;EndpointSuffix=core.windows.net"

        blob_service_client = BlobServiceClient.from_connection_string(
            connection_string
        )

        engine_output_container = blob_service_client.get_container_client(
            container_name
        )

        if not engine_output_container.exists():
            engine_output_container.create_container()

        return engine_output_container


class EngineOutputUploader(EngineOutputManager):
    def __init__(
        self,
        storage_account_name,
        storage_account_key,
        container_name,
        folder_to_upload,
    ):
        super().__init__(storage_account_name, storage_account_key, container_name)

        self.folder_to_upload = folder_to_upload

    def _upload_file(self, file_path, uploaded_file_name):
        blob_client = self.engine_output_container.get_blob_client(uploaded_file_name)
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data)

    # Function to upload a folder
    def _upload_folder(self, uploaded_folder_name):
        for root, _, files in (pbar := tqdm(os.walk(self.folder_to_upload))):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                uploaded_file_name = os.path.join(
                    uploaded_folder_name,
                    os.path.relpath(file_path, self.folder_to_upload),
                )
                pbar.set_description(f"Uploading {file_path} to {uploaded_file_name}")
                self._upload_file(file_path, uploaded_file_name)

    def upload_latest_output(self):
        self._upload_folder(
            datetime.now(pytz.timezone("Pacific/Auckland")).strftime(
                self.output_date_format
            )
        )


class EngineOutputDownloader(EngineOutputManager):
    def __init__(
        self,
        storage_account_name,
        storage_account_key,
        container_name,
        downloaded_folder_name,
    ):
        super().__init__(storage_account_name, storage_account_key, container_name)
        self.downloaded_folder_name = downloaded_folder_name

    def _get_latest_output(self):
        blob_names = list(self.engine_output_container.list_blobs())

        folders = list(set([f.name.split("/")[0] for f in blob_names]))

        dates = [
            datetime.strptime(date, self.output_date_format).astimezone(
                pytz.timezone("Pacific/Auckland")
            )
            for date in folders
        ]

        return folders[dates.index(max(dates))]

    def download_latest_output(self):
        latest_output_folder_name = self._get_latest_output()
        blobs = self.engine_output_container.list_blobs(
            name_starts_with=latest_output_folder_name
        )

        for blob in (pbar := tqdm(blobs)):
            blob_client = self.engine_output_container.get_blob_client(blob.name)
            pbar.set_description(
                f"Downloading {blob.name} to {self.downloaded_folder_name}"
            )
            download_path = os.path.join(
                self.downloaded_folder_name,
                os.path.relpath(blob.name, latest_output_folder_name),
            )
            if not os.path.exists(os.path.dirname(download_path)):
                os.makedirs(os.path.dirname(download_path))
            with open(download_path, "wb") as data:
                blob_client.download_blob().readinto(data)
