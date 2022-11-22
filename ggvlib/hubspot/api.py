import json
import os
import requests
from typing import List
from ggvlib.logging import logger
from ggvlib.hubspot.schemas import Contact
from ggvlib.parsing import chunks


class Client:
    """A base client for interacting with the Hubspot Legacy API
    Since they don't have Contact List integration for their new API, we'll have
    to use this
    """

    api_base_url = "https://api.hubapi.com"

    def __init__(self, access_token: str) -> None:
        """Accepts the access token of a private Hubspot app

        Args:
            access_token (str): The access token for the private app
        """
        self.access_token = access_token

    @property
    def auth_header(self) -> str:
        return {"Authorization": f"Bearer {self.access_token}"}

    @property
    def json_header(self) -> str:
        return {**self.auth_header, **{"Content-Type": "application/json"}}

    @classmethod
    def from_env(cls) -> "Client":
        if not os.getenv("HUBSPOT_ACCESS_TOKEN"):
            raise EnvironmentError(
                "The environment variable 'HUBSPOT_ACCESS_TOKEN' has not been set"
            )
        return Client(access_token=os.environ["HUBSPOT_ACCESS_TOKEN"])

    def _paginate_list(
        self, request_url: str, count: int, response_key: str, offset_key: str
    ) -> List[dict]:
        """Paginate through a list of reponse pages

        Args:
            request_url (str): The url to send a get request to
            count (int): The amount of items per list
            response_key (str): The item to fetch from the response
            offset_key (str): The pagination offset key

        Raises:
            RuntimeError: The request did not return a 200 status code

        Returns:
            List[dict]: A list of fetched responses
        """
        all_results = []
        has_more = True
        offset = 0
        offset_param_lookup = {"offset": "offset", "vid-offset": "vidOffset"}
        while has_more:
            response = requests.get(
                request_url,
                headers=self.auth_header,
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

    def get_object_properties(self, object_type: str) -> List[dict]:
        """List all properties for an object

        Args:
            object_type (str): The object to list properties for ie 'contacts'

        Returns:
            List[dict]: _description_

        >>> client = Client(os.environ["HUBSPOT_ACCESS_TOKEN"])
        >>> properties = client.get_object_properties("contacts")
        ... {
        ...        "name": "become_customer_date_db_",
        ...        "label": "Become Customer Date(DB)",
        ...        "description": "",
        ...        "groupName": "contact_activity",
        ...        "type": "date",
        ...        "fieldType": "date",
        ...        "fieldLevelPermission": None,
        ...        "referencedObjectType": None,
        ...        "externalOptionsReferenceType": None,
        ...        "optionSortStrategy": None,
        ...        "numberDisplayHint": "formatted",
        ...        "createdUserId": 111111111,
        ...        "searchableInGlobalSearch": False,
        ...        "hasUniqueValue": False,
        ...        "updatedUserId": 111111111,
        ...        "deleted": False,
        ...        "updatedAt": 1653230458358,
        ...        "readOnlyDefinition": False,
        ...        "formField": True,
        ...        "displayOrder": -1,
        ...        "readOnlyValue": False,
        ...        "mutableDefinitionNotDeletable": False,
        ...        "favorited": False,
        ...        "favoritedOrder": -1,
        ...        "calculated": False,
        ...        "externalOptions": False,
        ...        "displayMode": "current_value",
        ...        "showCurrencySymbol": False,
        ...        "hubspotDefined": None,
        ...        "hidden": False,
        ...        "currencyPropertyName": None,
        ...        "textDisplayHint": None,
        ...        "optionsAreMutable": None,
        ...        "searchTextAnalysisMode": None,
        ...        "isCustomizedDefault": False,
        ...        "options": [],
        ...        "createdAt": 1653230458358,
        ...    }
        """

        response = requests.get(
            f"{self.api_base_url}/properties/v2/{object_type}/properties",
            headers=self.auth_header,
        )
        if response.status_code == 200:
            return response.json()
        else:
            raise RuntimeError(response.status_code)

    def get_contact_lists(self, count: int = 10) -> List[dict]:
        """Return a list of all contact lists

        Args:
            count (int, optional): The number of contact lists per page. Defaults to 10.

        Returns:
            List[dict]: A list of contact lists
        """
        return self._paginate_list(
            request_url=f"{self.api_base_url}/contacts/v1/lists/",
            count=count,
            response_key="lists",
            offset_key="offset",
        )

    def get_contact_list(self, contact_list_id: int) -> dict:
        """Get information for a specific contact list by id

        Args:
            contact_list_id (int): The id of the contact list to fetch

        Raises:
            RuntimeError: The request did not return a 200 status code

        Returns:
            dict: Information about a contact list
        """
        response = requests.get(
            f"{self.api_base_url}/contacts/v1/lists/{contact_list_id}",
            headers=self.auth_header,
        )
        if response.status_code == 200:
            return response.json()
        else:
            raise RuntimeError(response.status_code)

    def get_contact_list_contacts(
        self,
        contact_list_id: int,
        count: int = 100,
        properties: List[str] = None,
    ) -> List[dict]:
        """Fetch a list of contact dicts from a contact list

        Args:
            contact_list_id (int): The contact list id
            count (int, optional): The amount of contacts to fetch per page. Defaults to 100.
            properties (List[str]): The list of properties to fetch for the contact

        Returns:
            List[dict]: A list of contacts from a contact list
        """
        if count > 100:
            raise ValueError(
                "The Hubspot API does not accept fetching more than 100 items at once"
            )
        url = f"{self.api_base_url}/contacts/v1/lists/{contact_list_id}/contacts/all"
        if properties:
            properties_query = "&property=".join(properties)
            url = f"{url}?property={properties_query}"
        return self._paginate_list(
            request_url=url,
            count=count,
            response_key="contacts",
            offset_key="vid-offset",
        )

    def add_contact_list_contacts(
        self,
        contact_list_id: str,
        emails: List[str] = [],
        vids: List[int] = [],
    ) -> dict:
        """Add contacts to a static contact list

        Args:
            contact_list_id (str): The contact list id
            emails (List[str], optional): A list of emails. Defaults to [].
            vids (List[int], optional): A list of vids. Defaults to [].

        Raises:
            RuntimeError: The response did not return a 200 status code

        Returns:
            dict: A JSON response from the client
        """

        if not emails and not vids:
            raise ValueError(
                "Either a list of contacts or vids is required for this endpoint"
            )

        response = requests.post(
            f"{self.api_base_url}/contacts/v1/lists/{contact_list_id}/add",
            headers=self.json_header,
            data=json.dumps({"vids": vids, "emails": emails}),
        )
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(response.json())
            raise RuntimeError(response.status_code)

    def create_or_update_contact(self, contact: Contact) -> dict:
        """Create or update a Contact

        Args:
            contact (Contact): A Hubspot contacts

        Raises:
            RuntimeError: The response did not return a 200 status code

        Returns:
            dict: The response from the Hubspot API
        """
        response = requests.post(
            f"{self.api_base_url}/contacts/v1/contact/createOrUpdate/email/{contact.email}",
            headers=self.json_header,
            data=json.dumps({"properties": contact.properties}),
        )
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(response.json())
            raise RuntimeError(response.status_code)

    def create_or_update_contact_batch(self, contacts: List[Contact]) -> None:
        """Create or update a list of Contacts

        Args:
            contacts (List[Contact]): A list of Hubspot contacts
        >>> new_contacts = [
                Contact(
                    email="a.person@gogox.com",
                    properties=[
                        {"property": "include_in_call_list", "value": False},
                    ],
                ),
                Contact(
                    email="another.person@gogox.com",
                    properties=[
                        {"property": "include_in_gge_call_list", "value": True},
                        {"property": "firstname", "value": "Another"},
                        {"property": "lastname", "value": "Person"},
                    ],
                ),
            ]
        """
        for contact_batch in list(chunks(contacts, 1000)):
            logger.info(f"Updating batch of {len(contact_batch)} contact(s)")
            response = requests.post(
                f"{self.api_base_url}/contacts/v1/contact/batch",
                headers=self.json_header,
                data=json.dumps([c.dict() for c in contacts]),
            )
            if response.status_code != 202:
                logger.error(response.json())