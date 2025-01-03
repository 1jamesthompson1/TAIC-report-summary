import os

import pandas as pd
import pytest

import engine.extract.ReportTypeAssignment as ReportTypeAssignment
import engine.utils.Modes as Modes


def test_report_type_assignment(tmpdir):
    report_event_types_path = os.path.join(
        pytest.output_config["folder_name"],
        pytest.output_config["all_event_types_df_file_name"],
    )
    report_titles_path = os.path.join(
        pytest.output_config["folder_name"],
        pytest.output_config["report_titles_df_file_name"],
    )
    parsed_report_path = os.path.join(
        pytest.output_config["folder_name"],
        pytest.output_config["parsed_reports_df_file_name"],
    )
    report_types_path = tmpdir.join("report_types.pkl")
    report_type_assigner = ReportTypeAssignment.ReportTypeAssigner(
        report_event_types_path,
        report_titles_path,
        parsed_report_path,
        report_types_path,
    )
    report_type_assigner.assign_report_types()

    assert os.path.exists(report_types_path)

    report_types_df = pd.read_pickle(report_types_path)

    assert report_types_df["type"].isna().sum() == 0


@pytest.mark.parametrize(
    "report_title, mode, expected_type",
    [
        pytest.param(
            "Hawker Beechcraft Corporation 1900D, ZK-EAQ cargo door opening in flight, Auckland International Airport, 9 April 2010",
            0,
            "Aircraft loading",
            id="aircraft_loading",
        ),
        pytest.param(
            "Passenger freight ferry 'Aratere,' steering malfunctions, Wellington Harbour and Queen Charlotte Sound, 9 February and 20 February 2005",
            2,
            "Machinery failure",
            id="machinery_failure",
        ),
        pytest.param(
            "Track warrant control irregularities, Woodville and Otane, 18 January 2005",
            1,
            "Safeworking Rule or Procedure Breach",
            id="safeworking_rule_or_procedure_breach",
        ),
        pytest.param(
            "Cessna 185A, ZK-CBY and Tecnam P2002, ZK-WAK Mid-air collision, near Masterton, 16 June 2019",
            0,
            "Aircraft separation",
            id="aircraft_separation",
        ),
    ],
)
def test_single_report_type_assignment(report_title, mode, expected_type):
    report_event_types_path = os.path.join(
        pytest.output_config["folder_name"],
        pytest.output_config["all_event_types_df_file_name"],
    )
    report_titles_path = os.path.join(
        pytest.output_config["folder_name"],
        pytest.output_config["report_titles_df_file_name"],
    )
    parsed_reports = os.path.join(
        pytest.output_config["folder_name"],
        pytest.output_config["parsed_reports_df_file_name"],
    )
    report_type_assigner = ReportTypeAssignment.ReportTypeAssigner(
        report_event_types_path, report_titles_path, parsed_reports, None
    )

    assert (
        report_type_assigner.assign_report_type(
            report_title, Modes.Mode(mode), False
        ).lower()
        == expected_type.lower()
    )
