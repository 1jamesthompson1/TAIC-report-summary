import os
import re

import pandas as pd
import roman
from pypdf import PdfReader, errors
from tqdm import tqdm

from ..utils.AzureStorage import PDFStorageManager


def convertPDFToText(
    parsed_reports_df_file_name, refresh, pdf_storage_manager: PDFStorageManager
):
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

    print("Using PDF storage manager (streaming from cloud)")
    print(f"Output file: {parsed_reports_df_file_name}")
    print(f"Refresh: {refresh}")

    # Get PDFs from cloud storage
    report_ids = pdf_storage_manager.list_pdfs()
    if not report_ids:
        print("No PDFs found in storage container.")
        return
    print(f"Found {len(report_ids)} PDFs in storage container")

    if os.path.exists(parsed_reports_df_file_name) and not refresh:
        parsed_reports_df = pd.read_pickle(parsed_reports_df_file_name)
    else:
        parsed_reports_df = pd.DataFrame(
            columns=["report_id", "text", "valid", "headers"]
        )

    new_parsed_reports = []

    print(
        f"Parsing {len(report_ids)} reports, there are currently {len(parsed_reports_df)} reports in the parsed reports dataframe"
    )

    for report_id in (pbar := tqdm(report_ids)):
        pbar.set_description(
            f"Extracting text from report PDFs, currently processing {report_id}"
        )

        # Skip if already processed
        if report_id in parsed_reports_df["report_id"].values:
            continue

        try:
            # Stream from storage
            temp_pdf_path = pdf_storage_manager.stream_pdf_to_temp_file(report_id)
            if temp_pdf_path is None:
                pbar.write(f"Failed to download {report_id} from storage")
                continue

            try:
                text, headers = extractTextFromPDF(temp_pdf_path)
            finally:
                # Clean up temporary file
                try:
                    os.unlink(temp_pdf_path)
                except Exception:
                    pass

            text, valid, pdf_to_internal_page_numbers = formatText(text, report_id)

            updated_headers = (
                updateHeaders(headers, pdf_to_internal_page_numbers)
                if not headers.empty
                else None
            )

            new_parsed_reports.append(
                {
                    "report_id": report_id,
                    "text": text,
                    "valid": valid,
                    "headers": updated_headers,
                }
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
            pbar.write(f"Error processing {report_id}: {e}")

    if len(new_parsed_reports) > 0:
        parsed_reports_df = pd.concat(
            [parsed_reports_df, pd.DataFrame(new_parsed_reports)], ignore_index=True
        )
        parsed_reports_df.to_pickle(parsed_reports_df_file_name)


def process_outline(outline, reader, level=1):
    headers = []
    if not outline:
        return headers
    for item in outline:
        if isinstance(item, list):
            headers.extend(process_outline(item, reader, level + 1))
        else:
            if isinstance(item["/Page"], dict) and "/StructParents" in item["/Page"]:
                headers.append(
                    {
                        "Title": item["/Title"],
                        "Level": level,
                        "Page": reader.get_destination_page_number(item),
                    }
                )

    return headers


def updateHeaders(headers, pdf_to_internal_page_numbers):
    converter = pd.DataFrame(pdf_to_internal_page_numbers).set_index(0)

    headers["Page"] = headers["Page"].apply(lambda x: converter.loc[x][1])

    return headers


def extractTextFromPDF(pdf_path):
    with open(pdf_path, "rb") as pdf_file:
        reader = PdfReader(pdf_file)
        text = ""
        for i, page in enumerate(reader.pages):
            try:
                text += f"\n\n--- Page {i} start ---\n" + page.extract_text()
            except errors.PdfReadError:
                text += f"\n\n--- Page {i} start ---\n"
                continue

        headers = pd.DataFrame(process_outline(reader.outline, reader))
    return text, headers


def formatText(text, report_id):
    """Format the string
    This function will take the raw PDF text extraction and format so that the rest of the engine can work with it.

    It will:
    - Replace the page numbers with << Page X >>, so that page numbers are easy to search
    - Replace uncommon characters with more common characters.

    """

    text = cleanText(text)

    page_number_matches = None
    match report_id[0:4]:
        case "ATSB":
            page_number_matches = getATSBPageNumbers(text)
        case "TSB_":
            page_number_matches = getTSBPageNumbers(text)
        case "TAIC":
            page_number_matches = getTAICPageNumbers(text)
        case _:
            raise ValueError(
                f"Unknown report type: {report_id[0:4]} for report '{report_id}'"
            )

    pdf_page_matches = list(
        re.finditer(r"^--- Page (\d+) start ---$", text, re.MULTILINE)
    )

    page_number_matches.sort(key=lambda x: x.span()[0])
    replacement_numbers = sync_page_numbers(page_number_matches, pdf_page_matches)

    valid_page_numbers = validate_page_numbers(replacement_numbers)

    # Perform the replacement of the PDF page numbers with the internal page numbers
    # This process leaves the original page numbers in the text
    results = []
    last_end = 0
    pdf_to_internal_page_numbers = list(zip(pdf_page_matches, replacement_numbers))
    for page_number_match, replacement_number in pdf_to_internal_page_numbers:
        start, end = page_number_match.span()
        results.append(text[last_end:start])
        if replacement_number != "":
            results.append(f"<< Page {replacement_number} >>")
        last_end = end

    results.append(text[last_end:])
    text = "".join(results)

    pdf_to_internal_page_numbers = [
        (int(pdf_page_match.group(1)), replacement_number)
        for (pdf_page_match, replacement_number) in pdf_to_internal_page_numbers
    ]

    return text, valid_page_numbers, pdf_to_internal_page_numbers


def getATSBPageNumbers(text):
    # Page numbers

    page_number_matches = list(
        re.finditer(
            r"^ ?[->][ ]{0,2}((\d{1,3})|([XVI]{1,4}))[ ]{0,2}[-<]",
            text,
            flags=re.IGNORECASE + re.MULTILINE,
        )
    )

    # Alot of the reports have page numbers which are easily identifiable. If not found then we do a more exhaustive search
    if len(page_number_matches) == 0:
        page_number_matches = list(
            re.finditer(
                r"^ ?(\d{1,3}|[XVI]{1,4}) ?$", text, flags=re.MULTILINE + re.IGNORECASE
            )
        )
        if all([match.group(1).isnumeric() for match in page_number_matches]):
            page_number_matches.extend(
                re.finditer(r"^ ?([xvi]{2,8}) ?[\w\W]{0,10}$", text, flags=re.MULTILINE)
            )
    else:
        page_number_matches.extend(
            re.finditer(r"^ ?([XVI]{1,8}) ?$", text, flags=re.MULTILINE + re.IGNORECASE)
        )

    return page_number_matches


def getTSBPageNumbers(text):
    page_number_matches = list(
        re.finditer(r"^ ?- ?(\d+) ?- ?$", text, flags=re.IGNORECASE + re.MULTILINE)
    )
    if len(page_number_matches) == 0:
        page_number_matches = list(
            re.finditer(
                r"[\|■] {0,2}(\d+) ?$", text, flags=re.IGNORECASE + re.MULTILINE
            )
        )

    return page_number_matches


def getTAICPageNumbers(text):
    page_number_matches = list(
        re.finditer(
            r"\|? ?Page {1,3}(\d+|[ivx]+) \|?(?!start)",
            text,
            flags=re.IGNORECASE + re.MULTILINE,
        )
    )

    return page_number_matches


def sync_page_numbers(page_number_matches: list, pdf_page_matches: list):
    """
    sync_page_numbers

    Parameters:
        page_number_matches (list): List of regex matches from the document's internal page numbers
        pdf_page_matches (list): List of regex matches from the PDF page numbers

    Returns:
        list: A list of replacement values for pdf_page_matches to update the PDF page numbers

    Description:
        This function synchronizes the PDF page numbers with the internal page numbers mentioned in the document.
        It takes two lists of regex matches as input and returns a list of replacement values to update the PDF page numbers.

    Example:
        Input:
            page_number_matches = ['i', 'ii', '3', '4', '7', '10']
            pdf_page_matches = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13']

        Output:
            ['', '', 'i', 'ii', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10']
    """

    # Default to just labelling the pages 1,n
    if len(page_number_matches) == 0:
        return [num + 1 for num in range(len(pdf_page_matches))]

    # Line up pdf page numbers with read page numbers. I need to figure out if there are pages before page one?

    # Filter out regex matched pages that are higher than actual PDF.
    page_number_matches = [
        match
        for match in page_number_matches
        if not match.group(1).isdecimal()
        or int(match.group(1))
        <= max(
            [
                int(match.group(1)) + 10 if match.group(1).isdecimal() else 0
                for match in pdf_page_matches
            ]
        )
    ]
    # Remove roman numerals that are found after multiple consecutive integers
    is_int = [match.group(1).isdecimal() for match in page_number_matches]

    found_start_int = False
    indices_to_delete = []
    for i in range(1, len(is_int)):
        if is_int[i] and is_int[i - 1]:
            if (
                int(page_number_matches[i - 1].group(1))
                == int(page_number_matches[i].group(1)) - 1
            ):
                found_start_int = True

        if not is_int[i] and found_start_int:
            indices_to_delete.append(i)

    if len(indices_to_delete) > 0:
        page_number_matches = [
            page_number_matches[i]
            for i in range(len(page_number_matches))
            if i not in indices_to_delete
        ]

    # Remove integers that are found before a roman numeral
    is_int = [match.group(1).isdecimal() for match in page_number_matches]
    if not all(is_int):
        last_numeral = max([i for i, x in enumerate(is_int) if not x])

        kept_indicies = [
            i
            for i, x in enumerate(is_int)
            if (x and i > last_numeral) or (not x and i <= last_numeral)
        ]
        page_number_matches = [page_number_matches[i] for i in kept_indicies]

    # Remove any duplicate matches that happen on the same page
    for i in range(len(pdf_page_matches) - 1):
        page_start = pdf_page_matches[i].start()
        page_end = pdf_page_matches[i + 1].start()

        matches_in_page = [
            match
            for match in page_number_matches
            if match.span()[0] >= page_start and match.span()[1] <= page_end
        ]
        if len(matches_in_page) > 1:
            for j in range(1, len(matches_in_page)):
                page_number_matches.remove(matches_in_page[j])

    # Find the first and last roman numeral and integer
    last_numeral = (
        max(
            i
            for i, match in enumerate(page_number_matches)
            if not match.group(1).isdecimal()
        )
        if not all(is_int)
        else -1
    )
    anchors = []
    if last_numeral != -1:
        anchors = [page_number_matches[0]]
        if last_numeral != 0:
            anchors.extend(
                [
                    page_number_matches[last_numeral],
                ]
            )

    if last_numeral != len(page_number_matches) - 1:
        anchors.extend(
            [
                page_number_matches[last_numeral + 1],
                page_number_matches[-1],
            ]
        )

    # Create a template that can be filled out for all of the correct page numbers.

    final_page_numbers = [None] * len(pdf_page_matches)
    for i in range(0, len(pdf_page_matches) - 1):
        if (
            pdf_page_matches[i].end()
            < anchors[0].start()
            < pdf_page_matches[i + 1].start()
        ):
            final_page_numbers[i] = anchors[0]
            anchors.pop(0)
            if len(anchors) == 0:
                break

        if (
            i == len(pdf_page_matches) - 2
            and pdf_page_matches[i + 1].end() < anchors[0].start()
        ):
            final_page_numbers[i + 1] = anchors[0]
            anchors.pop(0)
            break

    final_page_numbers = populate_final_page_numbers(final_page_numbers)

    return final_page_numbers


def populate_final_page_numbers(final_page_numbers):
    """
    This will take a list of None, roman numerals and integers. It will add '' before the roman numerals, fill in the gaps in the roman numerals and then fill out the integers
    """
    result = []
    current_roman_numeral = None
    current_int = None

    for i in range(0, len(final_page_numbers)):
        # Append the preceding empty string if needed
        if (
            (final_page_numbers[i] is None)
            and (not current_roman_numeral)
            and (not current_int)
        ):
            result.append("")
            continue

        # Found a none
        if final_page_numbers[i] is None:
            if current_int:
                current_int += 1
                result.append(current_int)
            elif current_roman_numeral:
                current_roman_numeral += 1
                result.append(roman.toRoman(current_roman_numeral).lower())

            continue

        # Found a roman numeral
        if not final_page_numbers[i].group(1).isdecimal():
            current_roman_numeral = roman.fromRoman(
                final_page_numbers[i].group(1).upper()
            )
            if current_roman_numeral > 1:
                fixing_index = i - 1
                fixing_current_roman = current_roman_numeral - 1
                while (
                    fixing_index >= 0
                    and fixing_current_roman > 0
                    and final_page_numbers[fixing_index] is None
                ):
                    result[fixing_index] = roman.toRoman(fixing_current_roman).lower()
                    fixing_current_roman -= 1
                    fixing_index -= 1
            result.append(roman.toRoman(current_roman_numeral).lower())
            continue

        # Found a digit
        if final_page_numbers[i].group(1).isdecimal():
            current_int = int(final_page_numbers[i].group(1))
            if current_int > 1 and i > 0 and isinstance(result[i - 1], str):
                # The first number found is not the first page. This could be an error in the PDF or the page number was missed. Therefore we will replace the result until we reach the start or roman numerals
                fixing_index = i - 1
                fixing_current_int = current_int - 1
                while (
                    fixing_index >= 0
                    and fixing_current_int > 0
                    and final_page_numbers[fixing_index] is None
                ):
                    result[fixing_index] = fixing_current_int
                    fixing_current_int -= 1
                    fixing_index -= 1
            result.append(current_int)
            continue

    return result


def validate_page_numbers(page_numbers: list) -> bool:
    """
    Takes a list of page_numbers and returns if they are valid and complete.

    There are five rules:
    1. Roman numerals before decimals
    2. No missing pages in the decimals
    3. only empty spaces before the roman numerals
    4. First decimal must be equal to `1` or `last_numeral + 1`
    5. Most use integers
    """
    last_roman_numeral = None
    last_int = None

    for page_number in page_numbers:
        # 3
        if page_number == "" and (last_roman_numeral or last_int):
            return False
        elif page_number == "":
            continue
        elif page_number is None:
            return False

        if isinstance(page_number, int):
            # 4
            if (
                last_roman_numeral
                and last_int is None
                and page_number not in [1, last_roman_numeral + 1]
            ):
                return False

            # 2
            if last_int and page_number != last_int + 1:
                return False

            last_int = page_number

            continue

        if isinstance(page_number, str):
            # 1
            if last_int:
                return False

            roman_num = roman.fromRoman(page_number.upper())

            if last_roman_numeral and roman_num != last_roman_numeral + 1:
                return False

            last_roman_numeral = roman_num
            continue

        raise NotImplementedError(
            f"It should never reach here page-number is: {page_number}"
        )

    # 5
    if not last_int:
        return False

    return True


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
