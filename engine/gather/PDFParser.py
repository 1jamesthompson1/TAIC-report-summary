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
    report_pdf_paths.sort()

    if os.path.exists(parsed_reports_df_file_name) and not refresh:
        parsed_reports_df = pd.read_pickle(parsed_reports_df_file_name)
    else:
        parsed_reports_df = pd.DataFrame(columns=["report_id", "text", "valid"])

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
                for i, page in enumerate(reader.pages):
                    text += page.extract_text() + "\n"

                text = cleanText(text)
                text, valid = formatText(text, report_id)

                new_parsed_reports.append(
                    {"report_id": report_id, "text": text, "valid": valid}
                )

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


def formatText(text, report_id):
    """Format the string
    This function will format the text so that the section headers and page numbers are easier to find.

    Specifically, the page numbers will be replaced with << Page number >>
    """

    match report_id[0:4]:
        case "ATSB":
            return formatATSBText(text, report_id)

        case "TSB":
            return formatTSBText(text, report_id)

        case "TAIC":
            return formatTAICText(text, report_id)


def formatATSBText(text, report_id):
    # Page numbers

    page_number_matches = list(
        re.finditer(
            r"^ ?[->][ ]{0,2}((\d{1,3})|([LXVI]{1,8}))[ ]{1,2}[-<]",
            text,
            flags=re.IGNORECASE + re.MULTILINE,
        )
    )

    # Alot of the reports have page numbers which are easily identifiable. If not found then we do a more exhaustive search
    if len(page_number_matches) == 0:
        page_number_matches = list(
            re.finditer(
                r"^ ?(\d{1,3}|[LXVI]{1,8]) ?$", text, flags=re.MULTILINE + re.IGNORECASE
            )
        )
    else:
        page_number_matches.extend(
            re.finditer(
                r"^ ?([LXVI]{1,8}) ?$", text, flags=re.MULTILINE + re.IGNORECASE
            )
        )

    print(f"Found {len(page_number_matches)} page numbers in {report_id}")
    print(f"Before formatting: {[match.group(1) for match in page_number_matches]}")
    page_number_matches, valid = validate_page_numbers(page_number_matches)

    results = []
    last_end = 0
    for page_number_match in page_number_matches:
        start, end = page_number_match.span()
        results.append(text[last_end:start])
        results.append(f"<< Page {page_number_match.group(1)} >>")
        last_end = end

    results.append(text[last_end:])
    text = "".join(results)

    return text, valid


def validate_page_numbers(page_number_matches: list):
    """This will take a list of regex matches. It will work out which ones are valid and which ones are not. It will try its best to clean up the invalid ones."""
    if len(page_number_matches) == 0:
        return page_number_matches, False
    elif len(page_number_matches) == 1:
        return page_number_matches, True

    # Check that roman numerals are only at the start

    is_int = [match.group(1).isdecimal() for match in page_number_matches]
    if not all(is_int):
        last_numeral = max([i for i, x in enumerate(is_int) if not x])
        kept_indicies = [i for i, x in enumerate(is_int) if x and i > last_numeral]

        page_number_matches = [page_number_matches[i] for i in kept_indicies]

    print([match.group(1) for match in page_number_matches])
    out_of_order_indicies = []

    # Check that the page numbers are increasing exclude missing pages
    for i in range(1, len(page_number_matches)):
        if not page_number_matches[i].group(1).isdecimal():
            i += 1
            continue
        if int(page_number_matches[i].group(1)) <= int(
            page_number_matches[i - 1].group(1)
        ):
            out_of_order_indicies.append(i)

    if len(out_of_order_indicies) > 0:
        print(
            f"Found {len(out_of_order_indicies)} out of order page numbers: {out_of_order_indicies}"
        )

    return page_number_matches, len(out_of_order_indicies) == 0


def formatTSBText(text, report_id):
    return text, True


def formatTAICText(text, report_id):
    # Clean up page numbers
    text = re.sub(
        r"(\| )?((Page) {1,3}(\d+))( \|)?",
        r"\n<< \3 \4 >>\n",
        text,
        flags=re.IGNORECASE,
    )
    return text, True


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
        ("›", ">"),
        ("‹", "<"),
    ]

    for character in characters_to_replace:
        text = text.replace(character[0], character[1])

    return text
