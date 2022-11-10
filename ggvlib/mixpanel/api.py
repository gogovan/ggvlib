import base64
from typing import Dict, List
import requests
import urllib.parse


class Client:
    """A base client for interacting with Mixpanel"""

    api_version = 2.0
    api_base_url = f"https://mixpanel.com/api/{api_version}/jql"

    def __init__(self, secret: str) -> None:
        """_summary_

        Args:
            account_sid (str): _description_
        """
        self.secret = secret

    @property
    def auth(self) -> str:
        return base64.b64encode(str.encode(self.secret)).decode("ascii")

    @property
    def headers(self) -> Dict[str, str]:
        return {"Authorization": f"Basic {self.auth}"}

    def run_jql_query(self, query: str) -> List[dict]:
        response = requests.post(
            self.api_base_url,
            data=urllib.parse.urlencode({"script": query}),
            headers=self.headers,
        )
        return response.json()
