import os

import pandas as pd
from tqdm import tqdm

import engine.utils.Modes as Modes
from engine.utils.AICaller import AICaller

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
            report_types_df = pd.DataFrame(columns=["report_id", "type", "title"])

        # Get all unassigned report_types
        merged_df = report_types_df.merge(
            self.report_titles_df, on=["report_id", "title"], how="outer"
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

        system_message = f"""
You are helping me extract and assign event types to reports based off their titles.

Can you please extract the accident event type from the report title.

Here is a list of the possible event types:
{mode_event_types_str}

Some events types overlap so make sure to read the entire list and choose the most specific one.

Your response will be a single event type without any other words.
"""

        user_message = f"""
Here are examples of what the classification should look like:

Extract event category from "Hawker Beechcraft Corporation 1900D, ZK-EAQ cargo door opening in flight, Auckland International Airport, 9 April 2010":
Aircraft Loading

Extract event category from "Chokyo Maru No.68, ran aground, Hauraki Gulf, New Zealand, 16 April 2024":
Grounding

Extract event category from "Cessna 152 ZK-ETY and Robinson R22 ZK-HGV, mid-air collision, Paraparaumu, 17 February 2008":
Collision

Extract event category from "{report_title}":

Here are the possible event types:
{mode_event_types_str}
"""

        type = AICaller.query(
            system=system_message,
            user=user_message,
            model="gpt-4",
            temp=0,
        )

        return type
