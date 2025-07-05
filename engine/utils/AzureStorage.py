import gc
import math
import os
import tempfile
import time
from datetime import datetime
from typing import List, Optional

import lancedb
import pandas as pd
import pyarrow as pa
import pytz
from azure.storage.blob import BlobServiceClient
from tqdm import tqdm


class AzureStorageBase:
    """
    Base class for Azure Blob Storage operations, providing common functionality
    for container management and blob operations.
    """

    def __init__(
        self, storage_account_name: str, storage_account_key: str, container_name: str
    ):
        self.storage_account_name = storage_account_name
        self.storage_account_key = storage_account_key
        self.container_name = container_name
        self.container = self._get_container()

    def _get_container(self):
        """Initialize and return the container client."""
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={self.storage_account_name};AccountKey={self.storage_account_key}==;EndpointSuffix=core.windows.net"

        blob_service_client = BlobServiceClient.from_connection_string(
            connection_string
        )
        container = blob_service_client.get_container_client(self.container_name)

        if not container.exists():
            container.create_container()

        return container

    def blob_exists(self, blob_name: str) -> bool:
        """Check if a blob exists in the container."""
        blob_client = self.container.get_blob_client(blob_name)
        return blob_client.exists()

    def upload_blob(self, blob_name: str, data: bytes, overwrite: bool = False) -> bool:
        """
        Upload data to a blob.

        Args:
            blob_name: The blob name
            data: The data to upload as bytes
            overwrite: Whether to overwrite existing blobs

        Returns:
            bool: True if uploaded, False if skipped (already exists and overwrite=False)
        """
        if not overwrite and self.blob_exists(blob_name):
            return False

        blob_client = self.container.get_blob_client(blob_name)
        blob_client.upload_blob(data, overwrite=overwrite)
        return True

    def download_blob(self, blob_name: str) -> Optional[bytes]:
        """
        Download a blob from the container.

        Args:
            blob_name: The blob name

        Returns:
            Optional[bytes]: Blob content or None if not found
        """
        try:
            blob_client = self.container.get_blob_client(blob_name)
            return blob_client.download_blob().readall()
        except Exception:
            return None

    def delete_blob(self, blob_name: str) -> bool:
        """
        Delete a blob from the container.

        Args:
            blob_name: The blob name

        Returns:
            bool: True if deleted, False if not found
        """
        try:
            blob_client = self.container.get_blob_client(blob_name)
            blob_client.delete_blob()
            return True
        except Exception:
            return False

    def get_blob_info(self, blob_name: str) -> Optional[dict]:
        """
        Get information about a blob in the container.

        Args:
            blob_name: The blob name

        Returns:
            Optional[dict]: Blob metadata or None if not found
        """
        try:
            blob_client = self.container.get_blob_client(blob_name)
            properties = blob_client.get_blob_properties()
            return {
                "name": blob_name,
                "size": properties.size,
                "last_modified": properties.last_modified,
                "etag": properties.etag,
            }
        except Exception:
            return None

    def list_blobs(self, name_starts_with: str = None) -> List[str]:
        """
        List all blob names in the container.

        Args:
            name_starts_with: Optional prefix filter

        Returns:
            List[str]: List of blob names
        """
        blob_names = self.container.list_blob_names(name_starts_with=name_starts_with)
        return list(blob_names)

    def upload_file(
        self, file_path: str, blob_name: str, overwrite: bool = False
    ) -> bool:
        """
        Upload a file to a blob.

        Args:
            file_path: Path to the file to upload
            blob_name: The blob name to upload to
            overwrite: Whether to overwrite existing blobs

        Returns:
            bool: True if uploaded, False if skipped
        """
        if not overwrite and self.blob_exists(blob_name):
            return False

        blob_client = self.container.get_blob_client(blob_name)
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=overwrite)
        return True

    def download_to_file(self, blob_name: str, file_path: str) -> bool:
        """
        Download a blob to a file.

        Args:
            blob_name: The blob name to download
            file_path: Path where to save the file

        Returns:
            bool: True if downloaded, False if not found
        """
        try:
            blob_client = self.container.get_blob_client(blob_name)

            # Ensure directory exists
            os.makedirs(os.path.dirname(file_path), exist_ok=True)

            with open(file_path, "wb") as data:
                blob_client.download_blob().readinto(data)
            return True
        except Exception:
            return False


class PDFStorageManager(AzureStorageBase):
    """
    Manages PDF storage in Azure Blob Storage, providing specialized methods
    for PDF operations with streaming capabilities.
    """

    def __init__(
        self, storage_account_name: str, storage_account_key: str, container_name: str
    ):
        super().__init__(storage_account_name, storage_account_key, container_name)
        self.pdf_list = [b for b in self.list_blobs() if b.endswith(".pdf")]

    @staticmethod
    def _report_id_to_blob_name(report_id: str) -> str:
        """Convert a report_id to the corresponding blob name."""
        return f"{report_id}.pdf"

    def pdf_exists(self, report_id: str) -> bool:
        """Check if a PDF already exists in the cached list of blobs."""
        blob_name = self._report_id_to_blob_name(report_id)
        return blob_name in self.pdf_list

    def upload_pdf(
        self, report_id: str, pdf_data: bytes, overwrite: bool = False
    ) -> bool:
        """
        Upload a PDF to the container.
        """
        blob_name = self._report_id_to_blob_name(report_id)
        return self.upload_blob(blob_name, pdf_data, overwrite)

    def download_pdf(self, report_id: str) -> Optional[bytes]:
        """
        Download a PDF from the container.
        """
        blob_name = self._report_id_to_blob_name(report_id)
        return self.download_blob(blob_name)

    def stream_pdf_to_temp_file(self, report_id: str) -> Optional[str]:
        """
        Stream a PDF to a temporary file and return the file path.
        The caller is responsible for cleaning up the temporary file.
        """
        pdf_data = self.download_pdf(report_id)
        if pdf_data is None:
            return None

        temp_fd, temp_path = tempfile.mkstemp(suffix=".pdf", prefix=f"{report_id}_")
        try:
            with os.fdopen(temp_fd, "wb") as temp_file:
                temp_file.write(pdf_data)
            return temp_path
        except Exception:
            try:
                os.unlink(temp_path)
            except Exception:
                pass
            raise

    def list_pdfs(self) -> List[str]:
        """
        List all PDF report IDs in the container.
        """
        blob_names = self.list_blobs()
        return [name[:-4] for name in blob_names if name.endswith(".pdf")]

    def delete_pdf(self, report_id: str) -> bool:
        """
        Delete a PDF from the container.
        """
        blob_name = self._report_id_to_blob_name(report_id)
        return self.delete_blob(blob_name)

    def get_pdf_info(self, report_id: str) -> Optional[dict]:
        """
        Get information about a PDF in the container.
        """
        blob_name = self._report_id_to_blob_name(report_id)
        info = self.get_blob_info(blob_name)
        if info:
            info["report_id"] = report_id
        return info


class EngineOutputManager(AzureStorageBase):
    """
    Manages engine output storage in Azure Blob Storage with specialized methods
    for organizing outputs by date and managing vector databases.
    """

    def __init__(
        self, storage_account_name: str, storage_account_key: str, container_name: str
    ):
        super().__init__(storage_account_name, storage_account_key, container_name)
        self.output_date_format = "%Y-%m-%d_%H:%M:%S"

    def _get_latest_output(self):
        """Get the latest output folder based on date."""
        print("Getting latest output folder...")
        blob_names = self.list_blobs()
        folders = list(set([f.split("/")[0] for f in blob_names if "/" in f]))

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
    """
    Handles uploading engine outputs including files and vector database embeddings.
    """

    def __init__(
        self,
        storage_account_name: str,
        storage_account_key: str,
        container_name: str,
        folder_to_upload: str,
        viewer_db_uri: str,
        safety_issues_embeddings_path_template: str,
        recommendation_embeddings_path_template: str,
        report_sections_embeddings_path_template: str,
        report_text_embeddings_path_template: str,
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

    def _upload_folder(self, uploaded_folder_name: str):
        """Upload a folder to the container."""
        items = os.walk(self.folder_to_upload)
        start_time = time.time()
        print(
            f"Going to upload folder {self.folder_to_upload} to {uploaded_folder_name} which has {len(list(items))} items"
        )
        for root, _, files in os.walk(self.folder_to_upload):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                blob_name = os.path.join(
                    uploaded_folder_name,
                    os.path.relpath(file_path, self.folder_to_upload),
                )
                self.upload_file(file_path, blob_name)
        print(
            f"Uploaded folder {self.folder_to_upload} to {uploaded_folder_name}, took {time.time() - start_time:.2f} seconds"
        )

    def _clean_embedding_dataframe(self, df, type):
        """Clean and standardize embedding dataframes."""
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

    def _upload_embedding_df(self, df, table, sample_frac):
        """Upload a dataframe to the vector database."""
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
        """Upload all embedding files to the vector database."""
        print(f"Uploading embeddings with sample_frac={sample_frac}")
        start_time = time.time()
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
        ]:
            embedding_file_chunks = [
                file_num
                for file_num in range(1, 1000)
                if os.path.exists(path_template.replace("{{num}}", str(file_num)))
            ]
            print(
                f"Uploading {type} embeddings, split into {len(embedding_file_chunks)} files"
            )
            df_start = time.time()

            for file_num in embedding_file_chunks:
                file_path = path_template.replace("{{num}}", str(file_num))
                df = pd.read_pickle(file_path)
                df = self._clean_embedding_dataframe(df, type)
                self._upload_embedding_df(df, table, sample_frac)
                del df
                gc.collect()

            print(f"Uploaded {type} embeddings in {time.time() - df_start:.2f}s")

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

        duration = time.time() - start_time
        print(f"Uploaded embeddings in {duration:.2f}s")

    def upload_latest_output(self):
        """Upload the latest output folder and embeddings."""
        print(f"=== Uploading latest output from {self.folder_to_upload} ===\n")
        output_date_str = datetime.now(pytz.timezone("Pacific/Auckland")).strftime(
            self.output_date_format
        )
        print(f"   Destination: {output_date_str}\n{'=' * 80}")

        self._upload_folder(output_date_str)
        self._upload_embeddings()


class EngineOutputDownloader(EngineOutputManager):
    """
    Handles downloading engine outputs from Azure Blob Storage.
    """

    def __init__(
        self,
        storage_account_name: str,
        storage_account_key: str,
        container_name: str,
        downloaded_folder_name: str,
    ):
        super().__init__(storage_account_name, storage_account_key, container_name)
        self.downloaded_folder_name = downloaded_folder_name

    def download_latest_output(self):
        """Download the latest output to the specified folder."""
        print(
            f"=== Downloading latest output to {self.downloaded_folder_name} ===\n\n{'=' * 80}"
        )
        latest_output_folder_name = self._get_latest_output()
        if latest_output_folder_name is None:
            print("No latest output found")
            return

        blobs = self.list_blobs(name_starts_with=latest_output_folder_name)

        print(f"Downloading {len(blobs)} blobs from {latest_output_folder_name}")

        successful = 0
        failed = 0
        start_time = time.time()
        for blob_name in blobs:
            if successful % 1000 == 0:
                print(f"Downloaded {successful} files so far...")
            download_path = os.path.join(
                self.downloaded_folder_name,
                os.path.relpath(blob_name, latest_output_folder_name),
            )
            try:
                self.download_to_file(blob_name, download_path)
            except Exception as e:
                print(f"Failed to download {blob_name}: {e}")
                failed += 1
                continue
            successful += 1

        duration = time.time() - start_time
        print(
            f"Downloaded {successful} files successfully, {failed} failed in {duration:.2f}s."
        )
