import gc
import math
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
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={storage_account_key};EndpointSuffix=core.windows.net"

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
        safety_issues_embeddings_path_template,
        recommendation_embeddings_path_template,
        report_sections_embeddings_path_template,
        report_text_embeddings_path_template,
        report_summary_embeddings_path_template,
    ):
        super().__init__(storage_account_name, storage_account_key, container_name)

        self.folder_to_upload = folder_to_upload
        self.db = lancedb.connect(viewer_db_uri)

        self.report_text_embeddings_path_template = report_text_embeddings_path_template
        self.recommendation_embeddings_path_template = (
            recommendation_embeddings_path_template
        )
        self.report_sections_embeddings_path_template = (
            report_sections_embeddings_path_template
        )
        self.safety_issues_embeddings_path_template = (
            safety_issues_embeddings_path_template
        )
        self.report_summary_embeddings_path_template = (
            report_summary_embeddings_path_template
        )

        self.vector_db_schema = pa.schema(
            [
                ("document_id", pa.string()),
                ("document", pa.string()),
                (
                    "vector",
                    pa.list_(pa.float64(), list_size=1024),
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

    def _clean_embedding_dataframe(self, df, type):
        match type:
            case "safety_issue":
                return (
                    df.rename(columns={"safety_issue_embedding": "vector"})
                    .drop(columns=["safety_issue_embedding_token_length"])[
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
                    ]
                    .assign(document_type=type)
                )
            case "recommendation":
                return df.rename(columns={"recommendation_embedding": "vector"})[
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
                ].assign(document_type=type)
            case "report_section":
                return df.rename(columns={"section_embedding": "vector"})[
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
                ].assign(document_type=type)
            case "report_text":
                return df.rename(columns={"text_embedding": "vector"})[
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
                ].assign(document_type=type)
            case "summary":
                return df.rename(columns={"summary_embedding": "vector"})[
                    [
                        "report_id",
                        "summary",
                        "vector",
                        "report_id",
                        "year",
                        "mode",
                        "agency",
                        "type",
                        "agency_id",
                        "url",
                    ]
                ].assign(document_type=type)

    def _upload_embedding_df(self, df, table, sample_frac):
        """
        This takes a dataframe and uploads it to the vector db
        """
        df = df.set_axis(
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

        df["document_id"] = df.apply(
            lambda row: row["document_id"]
            if (
                isinstance(row["document_id"], str)
                and row["document_id"].startswith(row["agency"])
            )
            else f"{row['agency']}_{row['document_id']}",
            axis=1,
        )
        # Converting to pyarrow first as it was having troubles giving a large pd.DataFrame directly
        pyarrow_table = pa.Table.from_pandas(
            df
            if sample_frac == 1
            else df.sample(frac=sample_frac, random_state=42, ignore_index=True),
            schema=self.vector_db_schema,
        )

        print(f"prepared pyarrow table size {pyarrow_table.nbytes/1024/1024:.2f} MB")

        # split the pyarrow table into chunks to uploaded separately

        average_row_size = pyarrow_table.nbytes / pyarrow_table.num_rows
        number_of_bytes_per_batch = 100_000_000  # 100 MB
        number_of_rows_per_batch = number_of_bytes_per_batch / average_row_size
        number_of_batches = math.ceil(pyarrow_table.num_rows / number_of_rows_per_batch)

        chunks = []
        for i in range(number_of_batches):
            start = i * number_of_rows_per_batch
            end = min(start + number_of_rows_per_batch, pyarrow_table.num_rows)
            chunk = pyarrow_table.slice(start, end - start)
            chunks.append(chunk)

        print(f"created {len(chunks)} chunks")

        for chunk in tqdm(chunks):
            table.add(chunk, mode="append")

        del pyarrow_table, chunks, df

    def _upload_embeddings(self, sample_frac=1):
        table = self.db.create_table(
            "all_document_types",
            data=None,
            schema=self.vector_db_schema,
            mode="overwrite",
        )

        for type, path_template in [
            ("report_text", self.report_text_embeddings_path_template),
            ("recommendation", self.recommendation_embeddings_path_template),
            ("report_section", self.report_sections_embeddings_path_template),
            ("safety_issue", self.safety_issues_embeddings_path_template),
            ("summary", self.report_summary_embeddings_path_template),
        ]:
            print(f"Uploading {type} embeddings")
            embedding_fie_chunks = [
                file_num
                for file_num in range(0, 1000)
                if os.path.exists(path_template.replace("{{num}}", str(file_num)))
            ]

            for file_num in (pbar := tqdm(embedding_fie_chunks)):
                file_path = path_template.replace("{{num}}", str(file_num))
                pbar.set_description(f"Uploading {file_path}")
                df = pd.read_pickle(file_path)
                df = self._clean_embedding_dataframe(df, type)
                self._upload_embedding_df(df, table, sample_frac)
                del df
                gc.collect()

        print("created vector db")

        table.cleanup_old_versions()

        print("cleaned up vector db")

        table.create_fts_index(
            "document",
            use_tantivy=False,
            language="English",
            stem=True,
            ascii_folding=True,
            replace=True,
        )

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

    def _should_download_file(self, blob, local_path):
        """
        Check if a file should be downloaded based on existence and modification time and that the file sizes match.

        Args:
            blob: Azure blob object with properties
            local_path: Local file path to check

        Returns:
            bool: True if file should be downloaded, False if it's up to date
        """
        # If local file doesn't exist, download it
        if not os.path.exists(local_path):
            return True

        if blob.size != os.path.getsize(local_path):
            print(
                f"File size mismatch: Blob size {blob.size} bytes, Local file size {os.path.getsize(local_path)} bytes"
            )
            return True

        # Get local file modification time
        local_mtime = datetime.fromtimestamp(
            os.path.getmtime(local_path), tz=pytz.timezone("Pacific/Auckland")
        )

        # Get blob's last modified time
        blob_mtime = blob.last_modified.astimezone(pytz.timezone("Pacific/Auckland"))

        # Download if blob is newer than local file
        return blob_mtime == local_mtime

    def download_latest_output(self, force_download=False):
        latest_output_folder_name = self._get_latest_output()
        if latest_output_folder_name is None:
            print("No latest output found")
            return
        blobs = self.engine_output_container.list_blobs(
            name_starts_with=latest_output_folder_name
        )

        downloaded_count = 0
        skipped_count = 0

        for blob in (pbar := tqdm(blobs)):
            blob_client = self.engine_output_container.get_blob_client(blob.name)
            download_path = os.path.join(
                self.downloaded_folder_name,
                os.path.relpath(blob.name, latest_output_folder_name),
            )

            # Check if file needs to be downloaded
            should_download = force_download or self._should_download_file(
                blob, download_path
            )

            if should_download:
                pbar.set_description(
                    f"Downloading {blob.name} to {self.downloaded_folder_name}"
                )
                if not os.path.exists(os.path.dirname(download_path)):
                    os.makedirs(os.path.dirname(download_path))
                with open(download_path, "wb") as data:
                    blob_client.download_blob().readinto(data)
                downloaded_count += 1
            else:
                pbar.set_description(f"Skipping {blob.name} (already up to date)")
                skipped_count += 1

        print(
            f"Download complete: {downloaded_count} files downloaded, {skipped_count} files skipped"
        )
