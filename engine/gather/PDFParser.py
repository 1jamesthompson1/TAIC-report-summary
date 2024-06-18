import os
import re

import pandas as pd
from pypdf import PdfReader
from tqdm import tqdm


def convertPDFToText(report_pdfs_folder, parsed_reports_df_file_name, refresh):
    print(
        "=============================================================================================================================\n"
    )
    print(
        "|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||\n"
    )
    print(
        "- - - - - - - - - - - - - - - - - - - - - - - - - - - - Converting PDFs to text - - - - - - - - - - - - - - - - - - - - - -"
    )
    print(
        "|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||\n"
    )
    print(
        "=============================================================================================================================\n"
    )
    print(f"Report PDFs folder: {report_pdfs_folder}")
    print(f"Output file: {parsed_reports_df_file_name}")
    print(f"Refresh: {refresh}")
    if not os.path.exists(report_pdfs_folder):
        print(
            "No reports have been downloaded so far. Please make sure that reports have been downloaded before running this function."
        )
        return

    report_pdf_paths = [
        os.path.join(report_pdfs_folder, pdf)
        for pdf in os.listdir(report_pdfs_folder)
        if pdf.endswith(".pdf")
    ]

    if os.path.exists(parsed_reports_df_file_name) and not refresh:
        parsed_reports_df = pd.read_pickle(parsed_reports_df_file_name)
    else:
        parsed_reports_df = pd.DataFrame(columns=["report_id", "text"])

    new_parsed_reports = []

    print(
        f"Parsing {len(report_pdf_paths)} reports, there are currently {len(parsed_reports_df)} reports in the parsed reports dataframe"
    )

    for report_pdf_path in (pbar := tqdm(report_pdf_paths)):
        pbar.set_description(
            f"Extracting text from report PDFs, currently processing {report_pdf_path}"
        )
        report_id = os.path.basename(os.path.normpath(report_pdf_path))[:-4]
        # Go into each folder and find the pdf
        if report_id in parsed_reports_df["report_id"].values:
            continue
        try:
            with open(report_pdf_path, "rb") as pdf_file:
                reader = PdfReader(pdf_file)
                text = ""
                for page in reader.pages:
                    text += page.extract_text() + "\n"

                text = formatText(text, "old")

                text = cleanText(text)

                new_parsed_reports.append({"report_id": report_id, "text": text})

                if len(new_parsed_reports) > 50:
                    parsed_reports_df = pd.concat(
                        [parsed_reports_df, pd.DataFrame(new_parsed_reports)],
                        ignore_index=True,
                    )
                    parsed_reports_df.to_pickle(parsed_reports_df_file_name)
                    pbar.write(
                        f"  Saving {len(new_parsed_reports)} reports to {parsed_reports_df_file_name}. There are now {len(parsed_reports_df)} reports in the parsed dataframe."
                    )
                    new_parsed_reports = []

        except Exception as e:
            pbar.write(f"Error processing {report_pdf_path}: {e}")

    if len(new_parsed_reports) > 0:
        parsed_reports_df = pd.concat(
            [parsed_reports_df, pd.DataFrame(new_parsed_reports)], ignore_index=True
        )
        parsed_reports_df.to_pickle(parsed_reports_df_file_name)


def formatText(text, style):
    """Format the string
    This will make the headers and page numbers easier to find.
    """

    # Clean up page numbers
    text = re.sub(
        r"(\| )?((Page) {1,3}(\d+))( \|)?",
        r"\n<< \3 \4 >>\n",
        text,
        flags=re.IGNORECASE,
    )

    return text


def cleanText(text):
    """Clean unusual characters from the report
    This will involve replaces all characters with the more usual ascii characters.
    """

    characters_to_replace = [
        ("–", "-"),
        ("’", "'"),
        ("‘", "'"),
        ("“", '"'),
        ("”", '"'),
    ]

    for character in characters_to_replace:
        text = text.replace(character[0], character[1])

    return text
