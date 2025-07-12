import gc
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import voyageai
from tenacity import retry, stop_after_attempt, wait_random_exponential
from tqdm import tqdm
from transformers import AutoTokenizer


class Embedder:
    def __init__(self):
        self.vo = voyageai.Client()

        self.model = "voyage-large-2-instruct"
        self.model_context_limit = 16_000
        self.model_batch_limit = 120_000

        self.tokenizer = AutoTokenizer.from_pretrained(
            f"voyageai/{self.model}", legacy=True
        )

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

    def embed_query(self, query: str) -> list[float]:
        return self.vo.embed(
            query, model=self.model, input_type="query", truncation=False
        ).embeddings[0]

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

        print(
            f"There are a total of {df[token_length_column_name].sum()} tokens in {len(df)} documents"
        )

        to_drop = pd.Series(df[token_length_column_name] < self.model_context_limit * 2)

        df = df.loc[to_drop]

        print(
            f"Dropping documents with more than {self.model_context_limit*2} tokens which is {len(to_drop) - sum(to_drop)} documents"
        )

        # check which new columns needs to be computed, i.e
        if embedding_column_name not in df.columns:
            df.loc[:, embedding_column_name] = None
        missing_embeddings = df[df[embedding_column_name].isna()]

        if len(missing_embeddings) == 0:
            print("No documents need to be embedded skipping")
            return df

        tqdm.write(
            f"There are {len(missing_embeddings)} missing embeddings with {len(df)} number of documents, total token length {missing_embeddings[token_length_column_name].sum()}"
        )

        # Split documents into batches based on max model batch size
        batches = []
        for batch_size in reversed(range(1, 100)):
            batches = [
                missing_embeddings.iloc[i : i + batch_size]
                for i in range(0, len(missing_embeddings), batch_size)
            ]

            # Check if any batch size is too big
            if (
                all(
                    [
                        batch[token_length_column_name].sum()
                        < (self.model_batch_limit * 0.95)
                        for batch in batches
                    ]
                )
                and len(batches) > 1
            ):
                batches = [batch[document_column_name].tolist() for batch in batches]
                break

        batch_size = len(batches[0])

        embeddings = [None] * len(missing_embeddings)

        with ThreadPoolExecutor(max_workers=3) as executor:
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

        print(
            f"  Finished embedding the documents there are {len(embeddings)} embeddings"
        )

        # Update the dataframe with the computed embeddings
        df.loc[missing_embeddings.index, embedding_column_name] = embeddings

        return df

    @retry(
        reraise=True,
        stop=stop_after_attempt(14),
        wait=wait_random_exponential(multiplier=1, min=1, max=250),
    )
    def embed_batch(self, batch):
        return self.vo.embed(
            texts=batch,
            model=self.model,
            input_type="document",
            truncation=True,
        ).embeddings

    def embed_dataframe(
        self,
        input: str | pd.DataFrame,
        document_column_name,
        output_file_path_template: str | None,
        current_output_df=None,
        output_file_start_num: int = 0,
    ):
        if isinstance(input, str):
            df = pd.read_pickle(input)
        elif isinstance(input, pd.DataFrame):
            df = input
        else:
            raise TypeError(
                f"Invalid input type: {type(input)} It should be either str or pd.DataFrame"
            )

        print(f"Looking at: {df.columns}")
        max_rows = 30000
        split_dfs = []
        # Check out how many rows the current dataframe cna hold
        if (
            isinstance(current_output_df, pd.DataFrame)
            and len(current_output_df) < max_rows
        ):
            current_output_df = pd.concat(
                [
                    current_output_df,
                    df.iloc[: max_rows - len(current_output_df)],
                ]
            )

            df = df.iloc[max_rows - len(current_output_df) :]
            split_dfs.append(current_output_df)

        # Split the dataframe into smaller dataframes not more than 30,000 rows
        total_rows = len(df)
        num_splits = (total_rows + max_rows - 1) // max_rows  # Ceiling division

        # Split the DataFrame
        for i in range(num_splits):
            start_idx = i * max_rows
            end_idx = min((i + 1) * max_rows, total_rows)
            split_df = df.iloc[start_idx:end_idx].copy()
            split_dfs.append(split_df)

        print(
            f"Splitting dataframe of size {total_rows} into {len(split_dfs)} dataframes"
        )

        embedded_indexes = []

        for num, split_df in (
            pbar := tqdm(enumerate(split_dfs, start=(output_file_start_num)))
        ):
            pbar.set_description(f"Embedding dataframe {num}")
            embedded_df = self.embed_documents(
                split_df,
                self.embed_batch,
                document_column_name,
                document_column_name + "_embedding",
            )
            embedded_df.to_pickle(
                output_file_path_template.replace("{{num}}", str(num))
            )
            embedded_indexes.extend(list(embedded_df.index))
            del embedded_df
            gc.collect()

        return pd.Series(embedded_indexes)

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
                        "report_id",
                        "type",
                        "mode",
                        "year",
                        "agency",
                        "agency_id",
                        "url",
                        dataframe_column_name,
                    ]
                ].dropna()

            # Drop unmatched
            dataframe_to_embed = dataframe_to_embed[
                ~dataframe_to_embed["report_id"].str.contains("nmatched")
            ]

            # Drop columns that are none or are empty strings
            dataframe_to_embed = dataframe_to_embed.dropna(
                subset=[document_column_name]
            )
            dataframe_to_embed = dataframe_to_embed[
                dataframe_to_embed[document_column_name].str.strip() != ""
            ]

            document_indexes = output_file_path_template.replace("{{num}}", "indexes")

            if os.path.exists(document_indexes):
                previously_embedded_indexes = pd.read_pickle(document_indexes)
            else:
                previously_embedded_indexes = pd.Series([])

            dataframe_to_embed = dataframe_to_embed.drop(
                previously_embedded_indexes, errors="ignore"
            )

            if len(dataframe_to_embed) == 0:
                print("No documents need to be embedded skipping")
                continue

            current_output_file_num = len(
                [
                    file_num
                    for file_num in range(0, 1000)
                    if os.path.exists(
                        output_file_path_template.replace("{{num}}", str(file_num))
                    )
                ]
            )
            if current_output_file_num == 0:
                current_output_file = None
            else:
                current_output_file = pd.read_pickle(
                    output_file_path_template.replace(
                        "{{num}}", str(current_output_file_num)
                    )
                )

            embedded_indexes = self.embed_dataframe(
                dataframe_to_embed,
                document_column_name,
                output_file_path_template,
                current_output_file,
                current_output_file_num,
            )

            previously_embedded_indexes = pd.concat(
                [previously_embedded_indexes, embedded_indexes], ignore_index=True
            )

            previously_embedded_indexes.to_pickle(document_indexes)
