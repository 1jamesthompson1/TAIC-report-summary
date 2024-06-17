import os

from ..utils import Config
from ..utils import Modes
import regex as re
from tqdm import tqdm


class OutputFolderReader:
    def __init__(self, output_folder=None):
        self.output_config = Config.ConfigReader().get_config()["engine"]["output"]
        if output_folder is None:
            self.output_folder = self.output_config.get("folder_name")
        else:
            self.output_folder = output_folder

    def _get_report_ids(self):
        directory_names = os.listdir(self.output_folder)

        def extract_report_id(dir_name):
            if match := re.search(r"\d{4}_\d{3}", dir_name):
                return match.group()
            else:
                return None

        report_ids = map(extract_report_id, directory_names)

        return list(filter(lambda ele: ele is not None, report_ids))

    def _get_report_dirs(self):
        report_dir_template = self.output_config.get("reports").get("folder_name")

        report_dirs = filter(
            lambda dir: os.path.isdir(dir),
            [
                os.path.join(
                    self.output_folder,
                    report_dir_template.replace(r"{{report_id}}", report_id),
                )
                for report_id in self._get_report_ids()
            ],
        )

        return report_dirs

    def __get_requested_files(self, report_dir, report_id, file_name_templates):
        retrieved_files = []

        for file_name_template in file_name_templates:
            text_path = os.path.join(
                report_dir, file_name_template.replace(r"{{report_id}}", report_id)
            )

            if not os.path.exists(text_path):
                tqdm.write(
                    f"  Could not find {text_path} for {report_id}, skipping report."
                )
                break

            with open(text_path, "r", encoding="utf-8", errors="replace") as f:
                retrieved_text = f.read()

            if len(retrieved_text) < 5:
                tqdm.write(
                    f"  Text file for {report_id} is too short, skipping report."
                )
                break

            retrieved_files.append(retrieved_text)

        return retrieved_files

    def _read_file_from_each_report_dir(
        self,
        file_name_templates: list[str] | str,
        processing_function,
        filter_report=lambda report_id: True,
    ):
        for report_dir, report_id in tqdm(
            zip(self._get_report_dirs(), self._get_report_ids())
        ):
            if not filter_report(report_id):
                continue

            file_name_templates = (
                file_name_templates
                if isinstance(file_name_templates, list)
                else [file_name_templates]
            )

            retrieved_files = self.__get_requested_files(
                report_dir, report_id, file_name_templates
            )

            if len(retrieved_files) != len(file_name_templates):
                continue

            processing_function(report_id, *retrieved_files)

    def process_reports_with_specific_files(
        self, processing_function, file_names, report_filter=None
    ):
        """ "
        This will go through each report and retrieve the files specified in the file_names list.
        Then it will call the processing function passing the retrieve files in order of list.
        This is designed as a helper function that wraps `_read_file_from_each_report_dir` such that the caller doesn't need to depend on the config.
        """
        file_name_templates = [
            self.output_config.get("reports").get(file_name) for file_name in file_names
        ]

        # Check to see if any are None
        if any(
            file_name_template is None for file_name_template in file_name_templates
        ):
            print("  File name templates could not all be found.")
            return

        if report_filter is not None:
            self._read_file_from_each_report_dir(
                file_name_templates, processing_function, report_filter
            )
        else:
            self._read_file_from_each_report_dir(
                file_name_templates, processing_function
            )

    # To centralize the problem of remembering the specific template name I have added these function that call the above function.

    def read_all_themes(self, processing_function, modes=Modes.all_modes):
        self.process_reports_with_specific_files(
            processing_function,
            ["themes_file_name"],
            lambda report_id: Modes.get_report_mode_from_id(report_id) in modes,
        )

    def read_all_summaries(self, processing_function):
        self.process_reports_with_specific_files(
            processing_function, ["weightings_file_name"]
        )

    def process_reports(self, processing_function, modes=Modes.all_modes):
        self.process_reports_with_specific_files(
            processing_function,
            ["text_file_name"],
            lambda report_id: Modes.get_report_mode_from_id(report_id) in modes,
        )
