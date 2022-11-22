from typing import Dict, List
import requests
import urllib.parse


class Client:
    """A base client for interacting with the Hubspot Legacy API"""

    api_version = 1
    api_base_url = "https://api.hubapi.com"

    def __init__(self, access_token: str) -> None:
        """_summary_

        Args:
            access_token (str): _description_
        """
        self.access_token = access_token

    @property
    def headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.access_token}"}

    def _paginate_list(
        self,
        request_url: str,
        count: int,
        response_key: str,
        offset_key: str,
    ) -> List[dict]:
        all_results = []
        has_more = True
        offset = 0
        offset_param_lookup = {"list": "list", "vid-offset": "vidOffset"}
        while has_more:
            response = requests.get(
                request_url,
                headers=self.headers,
                params={
                    "count": count,
                    offset_param_lookup[offset_key]: offset,
                },
            )
            if response.status_code == 200:
                response_data = response.json()
                has_more = response_data["has-more"]
                offset = response_data[offset_key]
                all_results.extend(response_data[response_key])
            else:
                raise RuntimeError(response.status_code)
        return all_results

    def get_contact_lists(self, count: int = 10) -> List[dict]:
        return self._paginate_list(
            request_url=f"{self.api_base_url}/contacts/v{self.api_version}/lists/",
            count=count,
            response_key="lists",
            offset_key="offset",
        )

    def get_contact_list(self, contact_list_id: int) -> dict:
        response = requests.get(
            f"{self.api_base_url}/contacts/v{self.api_version}/lists/{contact_list_id}",
            headers=self.headers,
        )
        if response.status_code == 200:
            return response.json()
        else:
            raise RuntimeError(response.status_code)

    def get_list_contacts(
        self, contact_list_id: int, count: int = 100
    ) -> dict:
        return self._paginate_list(
            request_url=f"{self.api_base_url}/contacts/v{self.api_version}/lists/{contact_list_id}/contacts/all",
            count=count,
            response_key="contacts",
            offset_key="vid-offset",
        )
