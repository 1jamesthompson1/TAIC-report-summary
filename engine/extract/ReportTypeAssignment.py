import os

import pandas as pd
from tqdm import tqdm

import engine.utils.Modes as Modes
from engine.utils.OpenAICaller import openAICaller

tqdm.pandas()


class ReportTypeAssigner:
    def __init__(
        self, report_event_type_df_path, report_titles_df_path, report_types_df_path
    ):
        self.report_types_df_path = report_types_df_path

        if os.path.exists(report_titles_df_path):
            self.report_titles_df = pd.read_pickle(report_titles_df_path)
        else:
            raise ValueError(f"{report_titles_df_path} does not exist")

        if os.path.exists(report_event_type_df_path):
            all_event_types = pd.read_pickle(report_event_type_df_path)
            self.event_types_for_mode = {
                mode: "\n".join(
                    [
                        f"- {event_type}"
                        for event_type in all_event_types.query(
                            f"mode == '{Modes.Mode.as_string(mode).lower()}'"
                        )["Value"].tolist()
                    ]
                )
                for mode in Modes.all_modes
            }
        else:
            raise ValueError(f"{report_event_type_df_path} does not exist")

    def assign_report_types(self):
        print("==================================================" * 2)
        print("------------------  Assigning report event types   -----------------")
        print("==================================================" * 2)
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

        print(
            f"There are {len(unassigned_df)} reports that need to be assigned types out of {len(merged_df)} total reports"
        )
        if len(unassigned_df) == 0:
            return
        unassigned_df["type"] = unassigned_df.progress_apply(
            lambda row: self.assign_report_type(
                row["title"], Modes.get_report_mode_from_id(row["report_id"])
            ),
            axis=1,
        )

        combined_df = pd.concat([assigned_df, unassigned_df], ignore_index=True)
        combined_df.to_pickle(self.report_types_df_path)

    def assign_report_type(self, report_title: str, mode: Modes.Mode):
        mode_event_types_str = self.event_types_for_mode[mode]

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

Your response should be just a singe event type without any other text.
""",
            model="gpt-4",
            temp=0,
        )

        print(f"Assigned report type: {type}")
        return type
