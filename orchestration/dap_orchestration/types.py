from dataclasses import dataclass


class DapSurveyType(str):
    pass


@dataclass
class FanInResultsWithTsvDir:
    fan_in_results: list[DapSurveyType]
    tsv_dir: str
