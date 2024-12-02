import os
from datetime import datetime

import lancedb
import pandas as pd
import pyarrow as pa
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

    def _get_latest_output(self):
        blob_names = list(self.engine_output_container.list_blobs())

        folders = list(set([f.name.split("/")[0] for f in blob_names]))

        dates = [
            datetime.strptime(date, self.output_date_format).astimezone(
                pytz.timezone("Pacific/Auckland")
            )
            for date in folders
        ]
        if len(dates) == 0:
            return None

        return folders[dates.index(max(dates))]


class EngineOutputUploader(EngineOutputManager):
    def __init__(
        self,
        storage_account_name,
        storage_account_key,
        container_name,
        folder_to_upload,
        viewer_db_uri,
        safety_issues_embeddings_path,
        recommendation_embeddings_path,
        report_sections_embeddings_path,
        report_text_embeddings_path,
    ):
        super().__init__(storage_account_name, storage_account_key, container_name)

        self.folder_to_upload = folder_to_upload
        self.db = lancedb.connect(viewer_db_uri)

        self.report_text_embeddings_path = report_text_embeddings_path
        self.recommendation_embeddings_path = recommendation_embeddings_path
        self.report_sections_embeddings_path = report_sections_embeddings_path
        self.safety_issues_embeddings_path = safety_issues_embeddings_path

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

    def _upload_embeddings(self):
        report_sections_embeddings = pd.read_pickle(
            self.report_sections_embeddings_path
        ).rename(columns={"section_embedding": "vector"})
        report_text_embeddings = pd.read_pickle(
            self.report_text_embeddings_path
        ).rename(columns={"text_embedding": "vector"})
        recommendation_embeddings = pd.read_pickle(
            self.recommendation_embeddings_path
        ).rename(columns={"recommendation_embedding": "vector"})
        safety_issue_embeddings = (
            pd.read_pickle(self.safety_issues_embeddings_path)
            .rename(columns={"safety_issue_embedding": "vector"})
            .drop(columns=["safety_issue_embedding_token_length"])
        )

        all_document_dfs = [
            safety_issue_embeddings[
                [
                    "safety_issue_id",
                    "safety_issue",
                    "vector",
                    "report_id",
                    "year",
                    "mode",
                    "agency",
                    "type",
                    "agency_id",
                    "url",
                ]
            ].assign(document_type="safety_issue"),
            report_sections_embeddings[
                [
                    "section",
                    "section_text",
                    "vector",
                    "report_id",
                    "year",
                    "mode",
                    "agency",
                    "type",
                    "agency_id",
                    "url",
                ]
            ].assign(document_type="report_section"),
            recommendation_embeddings[
                [
                    "recommendation_id",
                    "recommendation",
                    "vector",
                    "report_id",
                    "year",
                    "mode",
                    "agency",
                    "type",
                    "agency_id",
                    "url",
                ]
            ].assign(document_type="recommendation"),
            report_text_embeddings[
                [
                    "report_id",
                    "text",
                    "vector",
                    "report_id",
                    "year",
                    "mode",
                    "agency",
                    "type",
                    "agency_id",
                    "url",
                ]
            ].assign(document_type="important_text"),
        ]

        all_document_dfs = [
            df.set_axis(
                [
                    "document_id",
                    "document",
                    "vector",
                    "report_id",
                    "year",
                    "mode",
                    "agency",
                    "type",
                    "agency_id",
                    "url",
                    "document_type",
                ],
                axis=1,
            )
            for df in all_document_dfs
        ]

        all_document_types = pd.concat(all_document_dfs, axis=0, ignore_index=True)

        all_document_types["document_id"] = all_document_types.apply(
            lambda row: row["document_id"]
            if (
                isinstance(row["document_id"], str)
                and row["document_id"].startswith(row["agency"])
            )
            else f"{row['agency']}_{row['document_id']}",
            axis=1,
        )

        # Converting to pyarrow first as it was having troubles giving a large pd.DataFrame directly
        all_document_types = pa.Table.from_pandas(all_document_types)

        schema = pa.schema(
            [
                ("document_id", pa.string()),
                ("document", pa.string()),
                (
                    "vector",
                    pa.list_(
                        pa.float64(), list_size=len(all_document_types["vector"][0])
                    ),
                ),
                ("report_id", pa.string()),
                ("year", pa.int64()),
                ("mode", pa.string()),
                ("agency", pa.string()),
                ("type", pa.string()),
                ("agency_id", pa.string()),
                ("url", pa.string()),
                ("document_type", pa.string()),
            ]
        )

        print("prepare pyarrow table")

        table = self.db.create_table(
            "all_document_types", all_document_types, schema, mode="overwrite"
        )

        print("created vector db")

        table.create_fts_index("document", use_tantivy=False)

        print("created fts index")

    def upload_latest_output(self):
        self._upload_folder(
            datetime.now(pytz.timezone("Pacific/Auckland")).strftime(
                self.output_date_format
            )
        )

        self._upload_embeddings()


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

    def download_latest_output(self):
        latest_output_folder_name = self._get_latest_output()
        if latest_output_folder_name is None:
            print("No latest output found")
            return
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
