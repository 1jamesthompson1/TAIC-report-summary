from tqdm import tqdm
import voyageai
import pandas as pd
from tenacity import retry, wait_random_exponential
from transformers import AutoTokenizer

from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing
import os


class Embedder:
    def __init__(self):
        self.vo = voyageai.Client()

        self.tokenizer = AutoTokenizer.from_pretrained("voyageai/voyage")

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

    def embed_documents(
        self, df, embedding_function, document_column_name, embedding_column_name
    ):
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

        # Get document lengths
        token_length_column_name = f"{embedding_column_name}_token_length"
        df = self.tokenize_documents(df, document_column_name, token_length_column_name)

        df = df.loc[df[token_length_column_name] < 15_000]

        # check which new columns needs to be computed, i.e
        if embedding_column_name not in df.columns:
            df[embedding_column_name] = None
        missing_embeddings = df[df[embedding_column_name].isna()]

        if len(missing_embeddings) == 0:
            return df

        tqdm.write(
            f"There are {len(missing_embeddings)} missing embeddings with {len(df)} number of documents"
        )

        # Split documents into batches based on max batch size of 120000.
        batches = []
        for batch_size in reversed(range(1, 25)):
            batches = [
                missing_embeddings.iloc[i : i + batch_size]
                for i in range(0, len(missing_embeddings), batch_size)
            ]

            # Check if any batch size is too big
            if (
                all(
                    [
                        batch[token_length_column_name].sum() < 110_000
                        for batch in batches
                    ]
                )
                and len(batches) > 1
            ):
                batches = [batch[document_column_name].tolist() for batch in batches]
                break

        batch_size = len(batches[0])

        embeddings = [None] * len(missing_embeddings)

        with ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
            futures = {
                executor.submit(embedding_function, batch): i
                for i, batch in enumerate(batches)
            }

            for future in tqdm(as_completed(futures), total=len(futures)):
                batch_embeddings = future.result()
                batch_index = futures[future]
                # Place embeddings in the correct positions
                start_index = batch_index * batch_size
                end_index = start_index + len(batch_embeddings)
                embeddings[start_index:end_index] = batch_embeddings

        embeddings = pd.Series(embeddings, index=missing_embeddings.index)

        # Update the dataframe with the computed embeddings
        df.loc[missing_embeddings.index, embedding_column_name] = embeddings

        return df

    @retry(wait=wait_random_exponential(multiplier=1, min=1, max=60))
    def embed_batch(self, batch):
        return self.vo.embed(
            texts=batch,
            model="voyage-large-2-instruct",
            input_type="document",
            truncation=False,
        ).embeddings

    def embed_dataframe(
        self,
        input: str | pd.DataFrame,
        document_column_name,
        output_file_path: str | None,
    ):
        if isinstance(input, str):
            df = pd.read_pickle(input)
        elif isinstance(input, pd.DataFrame):
            df = input
        else:
            raise TypeError(
                f"Invalid input type: {type(input)} It should be either str or pd.DataFrame"
            )

        embedded_df = self.embed_documents(
            df,
            self.embed_batch,
            document_column_name,
            document_column_name + "_embedding",
        )
        if output_file_path is not None:
            embedded_df.to_pickle(output_file_path)
        else:
            return embedded_df

    def process_extracted_reports(self, extracted_df_path, embeddings_config):
        print(f"==================================================")
        print(f"---------------  Embedding reports  --------------")
        print(f"==================================================")

        extracted_df = pd.read_pickle(extracted_df_path)
        extracted_df["report_id"] = extracted_df.index
        extracted_df = extracted_df.reset_index(drop=True)

        for dataframe_column_name, document_column_name, output_file_path in (
            pbar := tqdm(embeddings_config)
        ):
            pbar.set_description(
                f"Embedding {dataframe_column_name} into {output_file_path}"
            )
            dataframe_to_embed = None
            if isinstance(
                extracted_df[dataframe_column_name].dropna().iloc[0], pd.DataFrame
            ):
                dataframe_to_embed = pd.concat(
                    list(extracted_df[dataframe_column_name].dropna()),
                    ignore_index=True,
                )
            else:
                dataframe_to_embed = extracted_df[
                    ["report_id", dataframe_column_name]
                ].dropna()

            if os.path.exists(output_file_path):
                previously_embedded_df = pd.read_pickle(output_file_path)
                dataframe_to_embed = dataframe_to_embed.merge(
                    previously_embedded_df,
                    on=list(dataframe_to_embed.columns),
                    how="outer",
                )

            self.embed_dataframe(
                dataframe_to_embed, document_column_name, output_file_path
            )
