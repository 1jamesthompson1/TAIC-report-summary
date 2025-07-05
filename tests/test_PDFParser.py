import os
import re

import pandas as pd
import pytest

import engine.gather.PDFParser as PDFParser


@pytest.mark.parametrize(
    "report_id, expected",
    [
        pytest.param(
            "ATSB_a_2007_030",
            [
                "i",
                "ii",
                "iii",
                "iv",
                "v",
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
                12,
                13,
                14,
                15,
                16,
                17,
                18,
                19,
                20,
                21,
                22,
            ],
            id="ATSB_a_2007_030 (Incorrect roman numeral matches in text)",
        ),
        pytest.param(
            "ATSB_a_2002_646",
            [
                "i",
                "ii",
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
                12,
                13,
                14,
                15,
                16,
                17,
                18,
                19,
                20,
            ],
            id="ATSB_a_2002_646 (Lenient regex match of roman numerals causing error)",
        ),
        pytest.param(
            "TSB_m_2021_A0041", [i for i in range(1, 57)], id="TSB_m_2021_A0041"
        ),
        pytest.param(
            "TSB_a_2011_F0012", [i for i in range(1, 20)], id="TSB_a_2011_F0012"
        ),
        pytest.param(
            "TAIC_r_2004_121",
            [
                "i",
                "ii",
                "iii",
                "iv",
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
                12,
                13,
                14,
                15,
                16,
            ],
            id="TAIC_r_2004_121",
        ),
        pytest.param(
            "TAIC_r_2014_103",
            ["i", "ii", "iii", "iv"] + list(range(1, 39)),
            id="TAIC_r_2014_103",
        ),
        pytest.param(
            "TAIC_a_2019_006",
            ["i", "ii", "iii", "iv", "v", "vi"] + list(range(1, 65)),
            id="TAIC_a_2019_006 (removing duplicate matches on the same page)",
        ),
    ],
)
def test_formatText(report_id, expected):
    text, headers = PDFParser.extractTextFromPDF(
        os.path.join(
            pytest.output_config["folder_name"],
            pytest.output_config["report_pdf_folder_name"],
            f"{report_id}.pdf",
        )
    )

    text, valid, _ = PDFParser.formatText(text, report_id)

    page_number_matches = list(
        re.finditer(
            r"^<< Page (\d+|[LXVI]{1,8}) >>$", text, re.MULTILINE + re.IGNORECASE
        )
    )

    assert valid

    matched = [
        int(match.group(1)) if match.group(1).isnumeric() else match.group(1)
        for match in page_number_matches
    ]
    print(f"found: {matched}")
    print(f"expected: {expected}")
    assert matched == expected


def test_PDFParser(tmpdir, test_pdf_storage_manager):
    """Test PDF parsing using the shared PDF storage manager fixture"""
    parsed_reports_df_file_name = os.path.join(
        tmpdir.strpath,
        pytest.output_config["parsed_reports_df_file_name"],
    )

    # Use the shared PDF storage manager instead of a local folder
    PDFParser.convertPDFToText(
        parsed_reports_df_file_name,
        refresh=True,
        pdf_storage_manager=test_pdf_storage_manager,
    )

    assert os.path.exists(parsed_reports_df_file_name)

    parsed_reports_df = pd.read_pickle(parsed_reports_df_file_name)
    print(parsed_reports_df)

    # Only assert if there are actually reports to process
    if not parsed_reports_df.empty:
        assert parsed_reports_df["valid"].all()
    else:
        print("No PDFs found in test container for processing")
