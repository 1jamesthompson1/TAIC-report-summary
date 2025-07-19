import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import ClassVar, List, Union

import lancedb
import numpy as np
import pandas as pd
import pyarrow as pa
from azure.ai.inference import EmbeddingsClient
from azure.core.credentials import AzureKeyCredential
from lancedb.embeddings import EmbeddingFunctionRegistry
from lancedb.embeddings.base import TextEmbeddingFunction
from lancedb.embeddings.registry import register
from lancedb.embeddings.utils import TEXT
from lancedb.pydantic import LanceModel, Vector
from tqdm import tqdm
from transformers import AutoTokenizer


@register("AzureAI")
class AzureAIEmbeddingFunction(TextEmbeddingFunction):
    """
    An embedding function that uses the AzureAI API

    https://learn.microsoft.com/en-us/python/api/overview/azure/ai-inference-readme?view=azure-python-preview

    Parameters
    ----------
    - name: str
        The name of the model to use. This should be set to the model you want to use for embeddings.
    - _ndims: int
        The number of dimensions of the embeddings. This is required to create the vector column in LanceDB.

    Also requires the following environment variables to be set:
    - AZURE_AI_ENDPOINT: The endpoint URL for the AzureAI service.
    - AZURE_AI_API_KEY: The API key for the AzureAI service.
    """

    name: str
    _ndims: int
    client: ClassVar = None

    def ndims(self):
        return self._ndims

    def compute_query_embeddings(self, query: str, *args, **kwargs) -> List[np.array]:
        return self.compute_source_embeddings(query, input_type="query")

    def compute_source_embeddings(self, texts: TEXT, *args, **kwargs) -> List[np.array]:
        texts = self.sanitize_input(texts)
        input_type = (
            kwargs.get("input_type") or "document"
        )  # assume source input type if not passed by `compute_query_embeddings`
        return self.generate_embeddings(texts, input_type=input_type)

    def generate_embeddings(
        self, texts: Union[List[str], np.ndarray], *args, **kwargs
    ) -> List[np.array]:
        """
        Get the embeddings for the given texts

        Parameters
        ----------
        texts: list[str] or np.ndarray (of str)
            The texts to embed
        input_type: Optional[str]

        truncation: Optional[bool]
        """
        AzureAIEmbeddingFunction._init_client()

        if isinstance(texts, np.ndarray):
            if texts.dtype != object:
                raise ValueError(
                    "AzureAIEmbeddingFunction only supports input of type `object` (i.e., list of strings) for numpy arrays."
                )
            texts = texts.tolist()

        # batch process so that no more than 96 texts are sent at once.
        batch_size = 96
        embeddings = []
        for i in range(0, len(texts), batch_size):
            rs = AzureAIEmbeddingFunction.client.embed(
                input=texts[i : i + batch_size], model=self.name, **kwargs
            )
            embeddings.extend(emb.embedding for emb in rs.data)
        return embeddings

    @staticmethod
    def _init_client():
        if AzureAIEmbeddingFunction.client is None:
            if (
                os.environ.get("AZURE_AI_ENDPOINT") is None
                or os.environ.get("AZURE_AI_API_KEY") is None
            ):
                raise ValueError(
                    "AzureAI client not initialized. Please set AZURE_AI_ENDPOINT, AZURE_AI_API_KEY, and AZURE_EMBEDDING_MODEL environment variables."
                )
            AzureAIEmbeddingFunction.client = EmbeddingsClient(
                endpoint=os.environ["AZURE_AI_ENDPOINT"],
                credential=AzureKeyCredential(os.environ["AZURE_AI_API_KEY"]),
            )


class VectorDB:
    """
    This is a VectorDB class that is used to add documents to the vector database.
    """

    def __init__(
        self,
        local_embedded_ids_path,
        db_uri: str = "~/.lancedb",
        model_name: str = "embed-v-4-0",
        embedded_length: int = 1536,
        context_limit: int = 128_000,
    ):
        self.local_embedded_ids_path = local_embedded_ids_path

        self.model_context_limit = context_limit

        self.table_name = "all_document_types"
        self.db = lancedb.connect(db_uri)
        azure_embeddings = (
            EmbeddingFunctionRegistry.get_instance()
            .get("AzureAI")
            .create(name=model_name, _ndims=embedded_length)
        )

        class VectorDBSchema(LanceModel):
            vector: Vector(azure_embeddings.ndims()) = azure_embeddings.VectorField()
            document: str = azure_embeddings.SourceField()
            document_id: str
            report_id: str
            year: int
            mode: int
            agency: str
            type: str
            agency_id: str
            url: str
            document_type: str

        self.VectorDBSchema = VectorDBSchema

        # using Cohere tokenizer for tokenization as a proxy to give me a rough guage.
        self.tokenizer = AutoTokenizer.from_pretrained(
            "Cohere/Cohere-embed-english-v3.0"
        )

    def _get_or_create_table(self):
        """Get existing table or create new one."""
        if self.table_name in self.db.table_names():
            return self.db.open_table(self.table_name)
        else:
            table = self.db.create_table(
                self.table_name, data=None, schema=self.VectorDBSchema, mode="create"
            )

            # Create FTS index for text search
            try:
                table.create_fts_index(
                    "document",
                    use_tantivy=False,
                    language="English",
                    stem=True,
                    ascii_folding=True,
                    replace=True,
                )
            except Exception as e:
                print(f"Warning: Could not create FTS index: {e}")

            try:
                table.create_scalar_index("document_id")
            except Exception as e:
                print(f"Warning: Could not create scalar index on document_id: {e}")

            return table

    def tokenize_documents(self, df, document_column_name, tokenization_column_name):
        if tokenization_column_name not in df.columns:
            df[tokenization_column_name] = df[document_column_name].apply(
                lambda x: len(self.tokenizer.tokenize(x))
            )
        else:
            df[tokenization_column_name] = df.apply(
                lambda x: len(self.tokenizer.tokenize(x[document_column_name]))
                if not isinstance(x[tokenization_column_name], int)
                else x[tokenization_column_name],
                axis=1,
            )

        return df

    def add_documents(self, df, document_column_name="document"):
        """
        Given a dataframe with atleast the document column and embedding column name it will generate embeddings for all of the documents that dont have embeddings in the dataframe.
        It does this by calling the embedding_function on batches of the documents.
        There is multithreading to speed up the process.

        Args:
            df: The dataframe
            embedding_function: The function that will be called to generate embeddings for documents. It must be`f([embedding]) -> [embedding]`
            batch_size: The size of the batches that will be passed to the embedding function
            document_column_name: The name of the column that contains the documents
            embedding_column_name: The name of the column that will contain the embeddings
        Returns the dataframe with the missing embeddings filled in.
        """

        # Check if the df follows the schema
        if df.columns.tolist() != list(self.VectorDBSchema.model_fields.keys())[1:]:
            raise ValueError(
                f"Dataframe columns {df.columns.tolist()} do not match the expected schema {list(self.VectorDBSchema.model_fields.keys())[1:]}"
            )

        # Get document lengths
        token_length_column_name = f"{document_column_name}_token_length"
        df = self.tokenize_documents(df, document_column_name, token_length_column_name)

        print(
            f"There are a total of {df[token_length_column_name].sum()} tokens in {len(df)} documents"
        )

        to_drop = pd.Series(df[token_length_column_name] < self.model_context_limit * 2)

        df = df.loc[to_drop]

        print(
            f"Dropping documents with more than {self.model_context_limit*2} tokens which is {len(to_drop) - sum(to_drop)} documents"
        )

        # Split documents into batches so that each batch is no bigger than 5 times the size of the context window.
        batches = []
        for batch_size in reversed(range(1, 100)):
            batches = [
                df.iloc[i : i + batch_size] for i in range(0, len(df), batch_size)
            ]

            # Check if any batch size is too big
            if (
                all(
                    [
                        batch[token_length_column_name].sum()
                        < (self.model_context_limit * 0.95)
                        for batch in batches
                    ]
                )
                and len(batches) > 1
            ):
                batches = [
                    batch.drop(token_length_column_name, axis=1) for batch in batches
                ]
                break

        batch_size = len(batches[0])

        def add_documents_to_db(batch: pd.DataFrame):
            pa_table = pa.Table.from_pandas(
                batch, schema=pa.schema(self._get_or_create_table().schema[1:])
            )

            results = (
                self._get_or_create_table()
                .merge_insert(on=["document_id"])
                .when_not_matched_insert_all()
                .execute(pa_table)
            )

            return results

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {
                executor.submit(add_documents_to_db, batch): i
                for i, batch in enumerate(batches)
            }

            for future in tqdm(as_completed(futures), total=len(futures)):
                merge_result = future.result()
                batch_index = futures[future]

                if merge_result.num_inserted_rows != batches[batch_index].shape[0]:
                    raise RuntimeError(
                        f"Warning: Batch {batch_index} inserted {merge_result.num_inserted_rows} rows, expected {batches[batch_index].shape[0]} rows."
                    )

        return df

    def clean_dataframes(
        self, dataframe_to_embed, dataframe_column_name, document_column_name
    ):
        """
        Cleans the dataframe to all have the right column names and formats.
        """
        match dataframe_column_name:
            case "recommendations":
                dataframe_to_embed = dataframe_to_embed.rename(
                    columns={"recommendation": "document"}
                )
                dataframe_to_embed["document_id"] = dataframe_to_embed.apply(
                    lambda row: f"{row['recommendation_id']}_{"rec"}_{row['report_id']}",
                    axis=1,
                )
                dataframe_to_embed["document_type"] = "recommendation"
            case "sections":
                dataframe_to_embed = dataframe_to_embed.rename(
                    columns={"section_text": "document"}
                )
                dataframe_to_embed["document_id"] = dataframe_to_embed.apply(
                    lambda row: f"{row['section_id']}_{"sec"}_{row['report_id']}",
                    axis=1,
                )
                dataframe_to_embed["document_type"] = "section"
            case "safety_issues":
                dataframe_to_embed = dataframe_to_embed.rename(
                    columns={"safety_issue": "document"}
                )
                dataframe_to_embed["document_id"] = dataframe_to_embed.apply(
                    lambda row: f"{row['safety_issue_id']}_{"si"}_{row['report_id']}",
                    axis=1,
                )
                dataframe_to_embed["document_type"] = "safety_issue"
            case "summary":
                dataframe_to_embed = dataframe_to_embed.rename(
                    columns={dataframe_column_name: "document"}
                )
                dataframe_to_embed["document_type"] = dataframe_column_name

                dataframe_to_embed["document_id"] = dataframe_to_embed.apply(
                    lambda row: f"{"sum"}_{row['report_id']}", axis=1
                )
            case _:
                raise ValueError(
                    f"Unknown document column name: {document_column_name}"
                )

        dataframe_to_embed = dataframe_to_embed[
            self.VectorDBSchema.model_fields.keys()[1:]
        ]

        dataframe_to_embed["agency_id"] = dataframe_to_embed["agency_id"].astype(str)
        dataframe_to_embed["mode"] = dataframe_to_embed["mode"].astype(int)
        dataframe_to_embed["type"] = dataframe_to_embed["type"].astype(str)

        # Drop unmatched
        # Drop unmatched documents and track count
        unmatched_count = dataframe_to_embed["report_id"].str.contains("nmatched").sum()
        dataframe_to_embed = dataframe_to_embed[
            ~dataframe_to_embed["report_id"].str.contains("nmatched")
        ]
        print(f"Dropped {unmatched_count} unmatched documents")

        # Drop columns that are none or are empty strings and track count
        initial_count = len(dataframe_to_embed)
        dataframe_to_embed = dataframe_to_embed.dropna(subset=["document"])
        dataframe_to_embed = dataframe_to_embed[
            dataframe_to_embed["document"].str.strip() != ""
        ]
        empty_document_count = initial_count - len(dataframe_to_embed)

        print(f"Dropped {empty_document_count} documents with empty/null content")

        return dataframe_to_embed

    def process_extracted_reports(self, extracted_df_path, embeddings_config):
        print("==================================================")
        print("---------------  Embedding reports  --------------")
        print("   Extracted reports: ", extracted_df_path)
        print(f"   Embeddings {len(embeddings_config)} dataframes")
        print(
            f"   Embeddings config: \n{chr(10).join([str(config) for config in embeddings_config])}"
        )
        print("==================================================")

        extracted_df = pd.read_pickle(extracted_df_path)

        for dataframe_column_name, document_column_name, output_file_path_template in (
            pbar := tqdm(embeddings_config)
        ):
            pbar.set_description(
                f"Embedding {dataframe_column_name} into {output_file_path_template}"
            )
            dataframe_to_embed = None
            if isinstance(
                extracted_df[dataframe_column_name].dropna().iloc[0], pd.DataFrame
            ):
                filtered_extracted_df = extracted_df.dropna(
                    subset=[dataframe_column_name]
                )
                dataframe_to_embed = pd.concat(
                    [
                        df.assign(
                            report_id=report_id,
                            type=type,
                            mode=mode,
                            year=year,
                            agency=agency,
                            agency_id=agency_id,
                            url=url,
                        )
                        for df, report_id, type, mode, year, agency, agency_id, url in zip(
                            filtered_extracted_df[dataframe_column_name],
                            filtered_extracted_df["report_id"],
                            filtered_extracted_df["type"],
                            filtered_extracted_df["mode"],
                            filtered_extracted_df["year"],
                            filtered_extracted_df["agency"],
                            filtered_extracted_df["agency_id"],
                            filtered_extracted_df["url"],
                        )
                    ],
                    ignore_index=True,
                )
            else:
                dataframe_to_embed = extracted_df[
                    [
                        dataframe_column_name,
                        *list(self.VectorDBSchema.model_fields.keys())[2:],
                    ]
                ].dropna()

            cleaned_df = self.clean_dataframes(
                dataframe_to_embed,
                dataframe_column_name,
                document_column_name,
            )

            if os.path.exists(self.local_embedded_ids_path):
                local_embedded_ids = pd.read_pickle(self.local_embedded_ids_path)
                cleaned_df = cleaned_df[
                    ~cleaned_df["document_id"].isin(local_embedded_ids["document_id"])
                ]
                print(
                    f"Filtered out {len(local_embedded_ids)} already embedded documents"
                )
            else:
                local_embedded_ids = pd.Series(dtype=str)

            added_document_ids = self.add_documents(
                dataframe_to_embed,
            )

            pd.concat(
                [local_embedded_ids, added_document_ids],
            ).to_pickle(self.local_embedded_ids_path)

            self._get_or_create_table().optimize()
            self._get_or_create_table().cleanup_old_versions()
