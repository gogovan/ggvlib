import base64
import json
import urllib.parse
import requests
import os
import aiohttp
import asyncio
from typing import Dict, List, Literal, Optional
from pydantic import BaseModel
from ggvlib.logging import logger
from ggvlib.twilio.parsing import to_form_field


class ContentType(BaseModel):
    category: Literal[
        "text",
        "media",
        "location",
        "list-picker",
        "quick-reply",
        "call-to-action",
        "card",
    ]
    body: str
    actions: List[Dict[str, str]]


class ContentCreateRequest(BaseModel):
    """A model for creating content via the Twilio content API

    Args:
        friendly_name (str): What to name the new content
        variables (Dict[str, str]): Variables in a dict format
        types Dict[str, Dict[str, str]]: The type of content to create
        language (str): The content language (defaults to 'en')
    >>> new_content = ContentRequest(
    ... friendly_name="test",
    ... variables={"1": "name", "2": "11"},
    ... types=[
    ...     ContentType(
    ...         category="text",
    ...         body="Hi {{1}}, Your flight will depart from gate {{2}}.",
    ...         actions=[{"id": "test_1", "title": "action_1"}]
    ...     )
    ... ],
    ... language="en",
    ... )
    """

    friendly_name: str
    variables: Dict[str, str]
    content_types: List[ContentType]
    language: str = "en"

    def to_dict(cls) -> dict:
        exclude = ["content_types"]
        return dict(
            **{k: v for k, v in cls.dict().items() if k not in exclude},
            **{
                "types": {
                    f"twilio/{t.category}": {
                        "body": t.body,
                        "actions": t.actions,
                    }
                    for t in cls.content_types
                }
            },
        )


class ContentSendRequest(BaseModel):
    messaging_service_sid: str
    content_sid: str = ""
    body: str = ""
    recipient_phone: str
    content_variables: Dict[str, str] = {}

    def to_dict(cls) -> dict:
        d = cls.dict()
        content = {
            "To": d["recipient_phone"],
            "From": d["messaging_service_sid"],
            "Body": d["body"],
            "ContentSid": d["content_sid"],
        }
        if cls.content_variables:
            content.update(
                {
                    "ContentVariables": json.dumps(d["content_variables"]),
                }
            )
        return content


class MessagingServiceUpdateRequest(BaseModel):
    sid: str
    friendly_name: Optional[str]
    inbound_request_url: Optional[str]
    inbound_method: Optional[str]
    fallback_url: Optional[str]
    fallback_method: Optional[str]
    status_callback: Optional[str]
    sticky_sender: Optional[bool]
    mms_converter: Optional[bool]
    smart_encoding: Optional[bool]
    scan_message_content: Optional[bool]
    fallback_to_long_code: Optional[bool]
    area_code_geomatch: Optional[bool]
    validity_period: Optional[int]
    synchronous_validation: Optional[bool]
    usecase: Optional[str]
    use_inbound_webhook_on_number: Optional[bool]

    def to_dict(cls) -> dict:
        d = cls.dict()
        content = {to_form_field(k): v for k, v in d.items() if v}
        if list(content.keys()) == ["Sid"]:
            required = [k for k in d.keys() if k != "sid"]
            raise ValueError(
                f"One of the following values is required: {required}"
            )
        return content


class Client:
    """A base client for interacting with Twilio"""

    def __init__(self, account_sid: str, api_key: str = None) -> None:
        """_summary_

        Args:
            account_sid (str): _description_
            api_key (str): _description_
        """
        self.api_key = api_key
        self.account_sid = account_sid

    @property
    def auth(self) -> str:
        """Creates an authorization header value

        Returns:
            str: A base64 encoded version of your account_sid + api_key
        """
        return base64.b64encode(
            bytes(f"{self.account_sid}:{self.api_key}", "utf-8")
        ).decode()

    @property
    def json_headers(self) -> dict:
        return {
            "Authorization": f"Basic {self.auth}",
            "Content-Type": "application/json",
        }

    @property
    def form_headers(self) -> dict:
        return {
            "Authorization": f"Basic {self.auth}",
            "Content-Type": "application/x-www-form-urlencoded",
        }

    @classmethod
    def from_env(cls) -> "Client":
        if not (api_key := os.getenv("TWILIO_AUTH_TOKEN")):
            raise ValueError(
                "'TWILIO_AUTH_TOKEN' is not set as an environment variable"
            )
        if not (account_sid := os.getenv("TWILIO_ACCOUNT_SID")):
            raise ValueError(
                "'TWILIO_ACCOUNT_SID' is not set as an environment variable"
            )
        return cls(account_sid, api_key)

    def post_urlencoded_form(
        self, url: str, accept_status_code: int, payload: BaseModel
    ) -> dict:
        """_summary_

        Args:
            url (str): _description_
            accept_status_code (int): _description_
            payload (BaseModel): _description_

        Raises:
            Exception: _description_

        Returns:
            dict: _description_
        """
        response = requests.post(
            url=url,
            headers=self.form_headers,
            data=urllib.parse.urlencode(payload.to_dict()),
        )
        if response.status_code == accept_status_code:
            return response.json()
        else:
            raise Exception(
                f"Client did not accept request: {response.json()}"
            )


class MessagingServiceApiClient(Client):
    api_version = 1
    api_base_url = f"https://messaging.twilio.com/v{api_version}/Services"

    def get_all(self) -> dict:
        """_summary_

        Returns:
            dict: _description_
        """
        return requests.get(
            url=self.api_base_url, headers=self.json_headers
        ).json()

    def get(self, service_sid: str) -> dict:
        """_summary_

        Args:
            service_sid (str): _description_

        Returns:
            dict: _description_
        """
        return requests.get(
            url=f"{self.api_base_url}/{service_sid}", headers=self.json_headers
        ).json()

    def update(self, payload: MessagingServiceUpdateRequest) -> dict:
        """_summary_

        Args:
            payload (MessagingServiceUpdateRequest): _description_

        Returns:
            dict: _description_
        """
        return self.post_urlencoded_form(
            url=f"{self.api_base_url}/{payload.sid}",
            payload=payload,
            accept_status_code=200,
        )


class MessagingApiClient(Client):
    api_base_url = "https://api.twilio.com/2010-04-01/Accounts"

    def send_content(self, payload: ContentSendRequest) -> dict:
        """Send content via a messaging service using the Content API

        Args:
            payload (ContentSendRequest): A request containing information about what
            content to send

        Raises:
            Exception: If the request is not accepted

        Returns:
            dict: Information about the sent message, such as whether or not it was accepted
        """
        return self.post_urlencoded_form(
            url=f"{self.api_base_url}/{self.account_sid}/Messages.json",
            payload=payload,
            accept_status_code=201,
        )

    def async_send_content(
        self, message_batch: List[ContentSendRequest]
    ) -> List[aiohttp.ClientResponse]:
        """A wrapper method for _async_send_content

        Args:
            message_batch (List[ContentSendRequest]): A list of requests containing information about what
            content to send

        Returns:
            List[aiohttp.ClientResponse]: A list of responses from the client
        """
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(
            self._async_send_content(message_batch=message_batch)
        )

    async def _async_send_content(
        self, message_batch: List[ContentSendRequest]
    ) -> List[aiohttp.ClientResponse]:
        """Send content asynchronously via a messaging service using the Content API

        Args:
            message_batch (List[ContentSendRequest]): A list of requests containing information about what
            content to send

        Returns:
            List[aiohttp.ClientResponse]: A list of responses from the client
        """
        tasks = []
        async with aiohttp.ClientSession(
            headers=self.form_headers, trust_env=True
        ) as session:
            for request in message_batch:
                tasks.append(
                    self._post_async(session=session, payload=request)
                )
            response_data = await asyncio.gather(*tasks)
            await session.close()
            return response_data

    async def _post_async(
        self, session: aiohttp.ClientSession, payload: ContentSendRequest
    ) -> aiohttp.ClientResponse:
        """_summary_

        Args:
            session (aiohttp.ClientSession): _description_
            payload (ContentSendRequest): _description_

        Returns:
            aiohttp.ClientResponse: _description_
        """
        body = urllib.parse.urlencode(payload.to_dict())
        post_url = f"{self.api_base_url}/{self.account_sid}/Messages.json"
        try:
            async with session.post(
                url=post_url,
                data=body,
            ) as response:
                await response.read()
                if response.status == 500:
                    logger.error(response)
                return response
        except Exception as e:
            logger.error(e)
            return body


class ContentApiClient(Client):
    api_version = 1
    api_base_url = f"https://content.twilio.com/v{api_version}/Content"

    def create_content(self, payload: ContentCreateRequest) -> dict:
        """Used for creating content with the Content API

        Args:
            payload (ContentCreateRequest): A content creation request

        Raises:
            Exception: If the request is not accepted

        Returns:
            dict: Information about the new content, such as its content_sid
        """
        response = requests.post(
            url=self.api_base_url,
            headers=self.json_headers,
            data=json.dumps(payload.to_dict()),
        )
        if response.status_code == 201:
            return response.json()
        else:
            raise Exception(
                f"Client did not accept request: {response.json()}"
            )
