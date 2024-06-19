import os

import pandas as pd

import engine.utils.Modes as Modes
from engine.utils.OpenAICaller import openAICaller


class ReportTypeAssigner:
    def __init__(
        self, report_event_type_df_path, report_titles_df_path, report_types_df_path
    ):
        self.report_types_df_path = report_types_df_path

        if os.path.exists(report_titles_df_path):
            self.report_titles_df = pd.read_pickle(report_titles_df_path)
        else:
            raise ValueError("report_titles_df_path cannot be None")

        if os.path.exists(report_event_type_df_path):
            self.report_event_type_df = pd.read_pickle(report_event_type_df_path)
        else:
            raise ValueError("report_event_type_df_path cannot be None")

    def assign_report_types(self):
        if os.path.exists(self.report_types_df_path):
            report_types_df = pd.read_pickle(self.report_types_df_path)
        else:
            report_types_df = pd.DataFrame(columns=["report_id", "type"])

        # Get all unassigned report_types
        merged_df = report_types_df.merge(
            self.report_titles_df, on="report_id", how="outer"
        )

        unassigned_df = merged_df[merged_df["type"].isna()]
        assigned_df = merged_df[~merged_df["type"].isna()]
        print(unassigned_df)
        if len(unassigned_df) == 0:
            return

        unassigned_df["type"] = unassigned_df.apply(
            lambda row: self.assign_report_type(
                row["title"], Modes.get_report_mode_from_id(row["report_id"])
            ),
            axis=1,
        )

        print(unassigned_df)
        print(merged_df)

        combined_df = pd.concat([assigned_df, unassigned_df], ignore_index=True)
        combined_df.to_pickle(self.report_types_df_path)

    def assign_report_type(self, report_title: str, mode: Modes.Mode):
        print(f"Assigning report type for {report_title}")

        mode_event_types = self.report_event_type_df.query(
            f"mode == '{Modes.Mode.as_string(mode).lower()}'"
        )["Value"].tolist()

        mode_event_types_str = "\n".join(
            [f"- {event_type}" for event_type in mode_event_types]
        )
        print(mode_event_types_str)
        type = openAICaller.query(
            system="""
You are helping me extract and assign event types to reports based off their titles.
""",
            user=f"""
Can you please extract the accident event type from the report title.

Here is a list of the possible event types:
{mode_event_types_str}

Here area few examples of what you are to do:
Input:
Hawker Beechcraft Corporation 1900D, ZK-EAQ cargo door opening in flight, Auckland International Airport, 9 April 2010
Output:
Aircraft Loading

Input:
Chokyo Maru No.68, ran aground, Hauraki Gulf, New Zealand, 16 April 2024
Output:
Grounding

Here is the report title:
{report_title}

Your should be just a singe event type without any other text.
""",
            model="gpt-4",
            temp=0,
        )

        print(f"Assigned report type: {type}")
        return type
