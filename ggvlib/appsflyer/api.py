from io import StringIO
from datetime import date
import pandas as pd
import requests
from ggvlib.logging import logger


class Client:
    base_url = "https://hq1.appsflyer.com/api/raw-data/export/app"
    report_types = ["installs_report", "in_app_events_report"]
    api_version = 5

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key

    @classmethod
    def from_google_secret_manager(
        cls: "Client", secret_path: str
    ) -> "Client":
        from ggvlib.google import secrets

        logger.info("Initializing client from Google Secret Manager")
        return cls(secrets.get_value(secret_path))

    def run_report(
        self, app_id: str, report_type: str, start: date, end: date
    ) -> pd.DataFrame:
        """Runs an 'installs' or 'in app events' report on the AppsFlyer API

        Args:
            app_id (str): The id of the app to download a report of
            report_type (str): The type of report to download. Must be installs_report or in_app_events_report
            start (date): The start date of the report
            end (date): The end date of the report

        Raises:
            ValueError: Invalid report type

        Returns:
            pd.DataFrame: A dataframe of the report
        """
        if report_type in self.report_types:
            logger.info(f"Running {report_type} for app_id: {app_id}")
            params = {
                "from": start,
                "to": end,
            }
            report_url = (
                f"{self.base_url}/{app_id}/{report_type}/v{self.api_version}"
            )
            response = requests.get(
                report_url,
                params=params,
                headers={"authorization": f"Bearer {self.api_key}"},
            )
            df = pd.read_csv(
                StringIO(response.content.decode("utf-8")),
                low_memory=False,
            )
            df.columns = [
                column.lower().replace(" ", "_") for column in df.columns
            ]
            return df
        else:
            raise ValueError(
                "Invalid report type. Please select from [installs_report, in_app_events_report]"
            )
