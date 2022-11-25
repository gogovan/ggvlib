from typing import Any, Dict, List, Optional, Union
from pydantic import validator
from pydantic import BaseModel
from jsonschema import validate

PROPERTY_ARRAY_SCHEMA = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "property": {"type": "string"},
            "value": {"type": ["string", "boolean", "integer"]},
        },
        "required": ["property", "value"],
    },
}


CRM_FILTER_ARRAY_SCHEMA = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "propertyName": {"type": "string"},
            "operator": {"type": "string"},
            "value": {},
        },
        "required": ["propertyName", "operator", "value"],
    },
}


class Contact(BaseModel):
    """A schema for updating contacts in the Hubspot API. You can provide either an email or vid
    along with a list of properties

    Args:
        vid (str): The vid of the contact
        email (str): The email of the contact

    Raises:
        ValueError: Invalid values

    >>> contact = Contact(
    ...     email="a.person@gogox.com",
    ...     properties=[
    ...         {"property": "include_in_call_list", "value": False},
    ...     ],
    ... )
    >>> another_contact = Contact(
    ...     vid=123,
    ...     properties=[
    ...         {"property": "include_in_call_list", "value": False},
    ...     ],
    ... )
    """

    vid: Optional[str] = None
    email: Optional[str] = None
    properties: List[Dict[str, Union[bool, str, int]]]

    @validator("email", always=True)
    def check_email_or_vid(cls, email, values):
        if not values.get("vid") and not email:
            raise ValueError("Either email or vid is required")
        return email

    @validator("properties", always=True)
    def check_properties(cls, properties):
        validate(properties, PROPERTY_ARRAY_SCHEMA)
        return properties
