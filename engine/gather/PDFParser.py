import os
import re

import pandas as pd
import roman
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
                    text += f"\n\n--- Page {i} start ---\n" + page.extract_text()

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

        case "TSB_":
            return formatTSBText(text, report_id)

        case "TAIC":
            return formatTAICText(text, report_id)

        case _:
            raise ValueError(
                f"Unknown report type: {report_id[0:4]} for report '{report_id}'"
            )


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
                r"^ ?(\d{1,3}|[LXVI]{1,8}) ?$", text, flags=re.MULTILINE + re.IGNORECASE
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
    pdf_page_matches = list(
        re.finditer(r"^--- Page (\d+) start ---$", text, re.MULTILINE)
    )
    replacement_numbers = sync_page_numbers(page_number_matches, pdf_page_matches)

    valid_page_numbers = validate_page_numbers(replacement_numbers)

    results = []
    last_end = 0
    for page_number_match, replacement_number in zip(
        pdf_page_matches, replacement_numbers
    ):
        start, end = page_number_match.span()
        results.append(text[last_end:start])
        if replacement_number != "":
            results.append(f"<< Page {replacement_number} >>")
        last_end = end

    results.append(text[last_end:])
    text = "".join(results)

    # for page_number_match in page_number_matches:
    #     text = re.sub(page_number_match.group(0), '', text, re.MULTILINE)

    return text, valid_page_numbers


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

    if len(page_number_matches) == 0:
        return []

    # Line up pdf page numbers with read page numbers. I need to figure out if there are pages before page one?
    print([match.group(1) for match in pdf_page_matches])
    print([match.group(1) for match in page_number_matches])

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

    # Remove integers that are found before a roman numeral
    is_int = [match.group(1).isdecimal() for match in page_number_matches]
    anchors = []
    if not all(is_int):
        last_numeral = max([i for i, x in enumerate(is_int) if not x])

        kept_indicies = [
            i
            for i, x in enumerate(is_int)
            if (x and i > last_numeral) or (not x and i <= last_numeral)
        ]
        page_number_matches = [page_number_matches[i] for i in kept_indicies]

    print(f"Cleaned page numbers: {[match.group(1) for match in page_number_matches]}")

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

    print(page_number_matches)
    print(anchors)
    print(pdf_page_matches)

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

    print(f"Template: {final_page_numbers}")
    final_page_numbers = populate_final_page_numbers(final_page_numbers)

    print(f"Cleaned pdf page numbers: {final_page_numbers}")

    return final_page_numbers


def populate_final_page_numbers(final_page_numbers):
    """
    This will take a list of None, roman numerals and integers. It will add '' before the roman numerals, fill in the gaps in the roman numerals and then fill out the integers
    """
    result = []
    current_roman_numeral = None
    current_int = None

    for i in range(0, len(final_page_numbers)):
        print(f"{i}: {result}")
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
                print(f"Adding {current_int} from None")
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
            if current_int > 1:
                # The first number found is not the first page. This could be an error in the PDF or the page number was missed.
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
            print(f"Adding {current_int} as looking at {final_page_numbers[i]}")
            result.append(current_int)
            print(result)
            continue

    return result


def validate_page_numbers(page_numbers: list) -> bool:
    """
    Takes a list of page_numbers and returns if they are valid and complete.

    There are four rules:
    1. Roman numerals before decimals
    2. No missing pages in the decimals
    3. only empty spaces before the roman numerals
    4. First decimal must be equal to `1` or `last_numeral + 1`
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

    return True


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
