import re
from enum import Enum


class Mode(Enum):
    a = 0  # Aviation
    r = 1  # Rail
    m = 2  # Marine

    @classmethod
    def as_string(cls, mode):
        if mode == cls.a:
            return "Aviation"
        elif mode == cls.r:
            return "Rail"
        elif mode == cls.m:
            return "Marine"
        else:
            return None

    @classmethod
    def as_char(cls, mode):
        return Mode.as_string(mode).lower()[0]


all_modes = [Mode.a, Mode.r, Mode.m]


def get_report_mode_from_id(report_id: str):
    if match := re.search(r"_([amr])_", report_id):
        report_mode = Mode[match.group(1)]
        return report_mode
    # Leaving in the old id format for backwards compatibility. Once integration of ATSB and TSB is complete and the test sets are updated this can be removed.
    elif match := re.search(r"(\d{4})_(\d{3})", report_id):
        report_mode = Mode(int(match.group(2)[0]))
        return report_mode
    else:
        return None
