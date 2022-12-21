import json
import os
import requests
from datetime import datetime, timedelta
from typing import List
from jsonschema import validate
from ggvlib.logging import logger
from ggvlib.hubspot.schemas import Contact, CRM_FILTER_ARRAY_SCHEMA
from ggvlib.parsing import chunks, datetime_to_millis


class Client:
    """A base client for interacting with the Hubspot Legacy API.
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
    def auth_header(self) -> dict:
        return {"Authorization": f"Bearer {self.access_token}"}

    @property
    def json_header(self) -> dict:
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

    def _paginate_crm_list(
        self, url: str, body_args: dict, method: str = "POST"
    ) -> List[dict]:
        """Paginate through a list of reponse pages

        Args:
            url (str): The url to send a get request to
            body_args (dict): The body info to send along with the request

        Returns:
            List[dict]: A list of fetched responses
        """
        after = 0
        failures = 0
        results = []
        while True:
            body_args.update({"after": after})
            if method == "POST":
                response = requests.post(
                    url,
                    headers=self.json_header,
                    data=json.dumps(body_args),
                )
            elif method == "GET":
                response = requests.get(
                    url,
                    headers=self.json_header,
                    params=body_args,
                )
            else:
                raise ValueError("Method must be 'POST' or 'GET'")
            if response.status_code == 200:
                response_data = response.json()
                if info := response_data.get("results"):
                    results.extend(info)
                else:
                    results.append(response_data)
                if paging := response_data.get("paging"):
                    after = paging["next"]["after"]
                else:
                    break
            else:
                failures += 1
                logger.error(response.json())
                if failures > 3:
                    break
        return results

    def _put(self, url: str) -> dict:
        response = requests.put(url, headers=self.auth_header)
        if response.status_code <= 400:
            return response.json()
        else:
            raise RuntimeError(response.content)

    def _post(self, url: str, body_args: dict) -> dict:
        response = requests.post(
            url,
            headers=self.json_header,
            data=json.dumps(body_args),
        )
        if response.status_code <= 400:
            return response.json()
        else:
            raise RuntimeError(response.content)

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

    def associate_custom_object(
        self,
        object_type: str,
        object_id: str,
        to_object_type: str,
        to_object_id: str,
        association_type: str,
    ) -> dict:
        """_summary_

        Args:
            object_type (str): _description_
            object_id (str): _description_
            to_object_type (str): _description_
            to_object_id (str): _description_
            association_type (str): _description_

        Returns:
            dict: _description_
        """
        return self._put(
            url=(
                f"{self.api_base_url}/crm/v3/objects/{object_type}/{object_id}/"
                f"associations/{to_object_type}/{to_object_id}/{association_type}"
            )
        )

    def create_custom_object_import(
        self, file_path: str, import_request: dict
    ) -> dict:
        """_summary_

        Args:
            file_path (str): _description_
            import_request (dict): _description_

        Returns:
            dict: _description_
        """
        file = {"file": open(file_path, "rb")}
        body_args = {"import_request"}
        response = requests.post(
            f"{self.api_base_url}/crm/v3/imports",
            headers=self.auth_header,
            files=file,
            data=body_args,
        )

    def create_custom_object_batch(
        self, object_type: str, inputs: List[dict]
    ) -> dict:
        """_summary_

        Args:
            object_type (str): _description_
            inputs (List[dict]): _description_

        Returns:
            dict: _description_
        """
        return self._post(
            url=f"{self.api_base_url}/crm/v3/objects/{object_type}/batch/create",
            body_args={"inputs": inputs},
        )

    def get_custom_object_batch(
        self, object_type: str, inputs: List[dict], properties: List[str] = []
    ) -> List[dict]:
        """_summary_

        Args:
            object_type (str): _description_
            inputs (List[dict]): _description_
            properties (List[str], optional): _description_. Defaults to [].

        Returns:
            List[dict]: _description_
        """
        body_args = {"properties": properties, "inputs": inputs}
        return self._paginate_crm_list(
            url=f"{self.api_base_url}/crm/v3/objects/{object_type}/batch/read",
            body_args=body_args,
            method="POST",
        )

    def get_custom_object_schema(
        self, object_type: str = None, properties: List[str] = []
    ) -> List[dict]:
        """Returns calls

        Args:
            filters (List[dict]): A list of filters
            properties (List[str]): A list of properties to return

        Returns:
            List[dict]: A list of calls
        """
        body_args = {
            "properties": properties,
            "limit": 100,
        }
        if object_type:
            return self._paginate_crm_list(
                url=f"{self.api_base_url}/crm/v3/schemas/{object_type}",
                body_args=body_args,
                method="GET",
            )
        else:
            return self._paginate_crm_list(
                url=f"{self.api_base_url}/crm/v3/schemas/",
                body_args=body_args,
                method="GET",
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

    def get_contact_batch(
        self, emails: List[str], properties: List[str] = None
    ) -> List[dict]:
        """Fetch a list of contact dicts by email in batches of 100

        Args:
            emails (List[str]): The emails of the contacts to fetch
            properties (List[str]): The list of properties to fetch for the contact

        Returns:
            List[dict]: A list of contacts
        """
        responses = []
        url = f"{self.api_base_url}/contacts/v1/contact/emails/batch"
        if properties:
            properties_query = "&property=".join(properties)
            url = f"{url}?property={properties_query}"
        for email_batch in list(chunks(emails, 100)):
            logger.info(f"Fetching batch of {len(email_batch)} contact(s)")
            response = requests.get(
                f"{url}?email={'&email='.join(email_batch)}",
                headers=self.auth_header,
            )
            if response.status_code != 200:
                logger.error(response)
            else:
                response_json = response.json()
                formatted_json = [
                    response_json[key] for key in response_json.keys()
                ]
                responses.extend(formatted_json)
        return responses

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

    def get_calls_after(
        self,
        after: datetime,
        properties: List[str] = [],
        filters: List[dict] = [],
    ) -> List[dict]:
        """Returns all calls after a certain datetime

        Args:
            after (datetime): The datetime to return calls after
            properties (List[str], optional): A list of properties. Defaults to [].
            filters (List[dict], optional): A list of filters. Defaults to [].

        Returns:
            List[dict]: A list of calls
        """

        filters.append(
            {
                "propertyName": "hs_timestamp",
                "operator": "GTE",
                "value": datetime_to_millis(after),
            }
        )

        return self.get_calls(filters=filters, properties=properties)

    def get_calls(
        self, filters: List[dict] = [], properties: List[str] = []
    ) -> List[dict]:
        """Returns calls

        Args:
            filters (List[dict]): A list of filters
            properties (List[str]): A list of properties to return

        Returns:
            List[dict]: A list of calls
        """
        validate(filters, CRM_FILTER_ARRAY_SCHEMA)
        body_args = {
            "filterGroups": [{"filters": filters}],
            "properties": properties,
            "limit": 100,
        }
        return self._paginate_crm_list(
            url=f"{self.api_base_url}/crm/v3/objects/calls/search",
            body_args=body_args,
        )

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
        ...        Contact(
        ...            email="a.person@gogox.com",
        ...            properties=[
        ...                {"property": "include_in_call_list", "value": False},
        ...            ],
        ...        ),
        ...        Contact(
        ...            email="another.person@gogox.com",
        ...            properties=[
        ...                {"property": "include_in_gge_call_list", "value": True},
        ...                {"property": "firstname", "value": "Another"},
        ...                {"property": "lastname", "value": "Person"},
        ...            ],
        ...        ),
        ...    ]
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
