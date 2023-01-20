from typing import Dict, List
from googleapiclient.discovery import build, Resource
from jsonschema import validate
from ggvlib.google.schemas import PERSON, PERSON_BATCH
from ggvlib.google.utils import fetch_user_creds

SCOPES = ["https://www.googleapis.com/auth/contacts"]

DEFAULT_FIELDS = ["names", "emailAddresses", "phoneNumbers"]


def _client() -> Resource:
    """Returns an authorized Google People API service

    Returns:
        Resource: A Google People API service
    """
    return build("people", "v1", credentials=fetch_user_creds(scopes=SCOPES))


def get(resource_name: str, person_fields: List[str] = DEFAULT_FIELDS) -> dict:
    """_summary_

    Args:
        resource_name (str): _description_
        person_fields (List[str], optional): _description_. Defaults to DEFAULT_FIELDS.

    Returns:
        dict: _description_
    """
    return (
        _client()
        .people()
        .get(resourceName=resource_name, personFields=",".join(person_fields))
        .execute()
    )


def get_page(
    person_fields: List[str] = DEFAULT_FIELDS,
    page_size: int = 100,
    page_token: str = None,
) -> dict:
    """_summary_

    Args:
        person_fields (List[str], optional): _description_. Defaults to DEFAULT_FIELDS.
        page_size (int, optional): _description_. Defaults to 100.
        page_token (str, optional): _description_. Defaults to None.

    Returns:
        dict: _description_
    """
    return (
        _client()
        .people()
        .connections()
        .list(
            resourceName="people/me",
            pageSize=page_size,
            pageToken=page_token,
            personFields=",".join(person_fields),
        )
        .execute()
    )


def get_all(
    fields: List[str] = DEFAULT_FIELDS,
    page_size: int = 100,
) -> list[dict]:
    """_summary_

    Args:
        fields (List[str], optional): _description_. Defaults to DEFAULT_FIELDS.
        page_size (int, optional): _description_. Defaults to 100.

    Returns:
        list[dict]: _description_
    """
    all_results: List[Dict[str, any]] = []
    next_page_token = None
    while results := get_page(fields, page_size, next_page_token):
        all_results.extend(results["connections"])
        if next_page_token := results.get("nextPageToken"):
            continue
        else:
            break
    return all_results


def create(person: dict) -> dict:
    """Create a contact with the People API

    Args:
        person (dict): A dictionary containing the person's information

    Returns:
        dict: The newly created contact for the person
    """
    validate(person, PERSON)
    return _client().people().createContact(body=person).execute()


def update(
    resource_name: str,
    person: dict,
    update_person_fields: List[str] = DEFAULT_FIELDS,
) -> dict:
    """Update a contact with the People API

    Args:
        resource_name (str): The resource name of the person to update
        person (dict): A dictionary containing the person's updated information
        update_person_fields (List[str], optional): A list of fields to update. Defaults to DEFAULT_FIELDS.

    Returns:
        dict: The updated contact

    >>> person = get("people/c1234")
    >>> person.update({"emailAddresses": [{"value": "65465464@abcde.com"}]})
    >>> update(
    ... resource_name="people/c2532810010726254982",
    ...     person=person,
    ...     update_person_fields=["emailAddresses"],
    ... )
    """
    validate(person, PERSON)
    return (
        _client()
        .people()
        .updateContact(
            resourceName=resource_name,
            body=person,
            updatePersonFields=",".join(update_person_fields),
        )
        .execute()
    )


def update_batch(
    people: List[dict[str, any]],
    update_person_fields: List[str] = DEFAULT_FIELDS,
    fields: List[str] = DEFAULT_FIELDS,
):
    """_summary_

    Args:
        people (List[dict[str, any]]): _description_
        update_person_fields (List[str], optional): _description_. Defaults to DEFAULT_FIELDS.
        fields (List[str], optional): _description_. Defaults to DEFAULT_FIELDS.

    Returns:
        _type_: _description_

    """
    return (
        _client()
        .people()
        .batchUpdateContacts(
            body={
                "contacts": {
                    person["resourceName"]: person for person in people
                },
                "readMask": ",".join(update_person_fields),
                "updateMask": ",".join(fields),
            }
        )
        .execute()
    )


def delete(resource_name: str) -> None:
    """_summary_

    Args:
        resource_name (str): _description_

    Returns:
        _type_: _description_
    """
    return (
        _client().people().deleteContact(resourceName=resource_name).execute()
    )


def delete_batch(resource_names: List[str]) -> None:
    """_summary_

    Args:
        resource_names (List[str]): _description_

    Returns:
        _type_: _description_
    """
    return (
        _client()
        .people()
        .batchDeleteContacts(body={"resourceNames": resource_names})
        .execute()
    )


def delete_all() -> List[str]:
    """_summary_

    Returns:
        List[str]: _description_
    """
    all_resource_names = [r["resourceName"] for r in get_all()]
    delete_batch(resource_names=all_resource_names)
    return all_resource_names


def create_batch(
    people: List[Dict[str, any]], fields: List[str] = ["names"]
) -> dict:
    """_summary_

    Args:
        people (List[Dict[str, any]]): _description_
        fields (List[str], optional): _description_. Defaults to ["names"].

    Returns:
        dict: _description_

    >>> people_to_create = [
    ...    {
    ...        "contactPerson": {
    ...            "names": [{"givenName": "bob"}],
    ...            "phoneNumbers": [{"value": "12345"}],
    ...        }
    ...    },
    ...    {
    ...        "contactPerson": {
    ...            "names": [{"givenName": "bill"}],
    ...            "phoneNumbers": [{"value": "678910"}],
    ...        }
    ...    }
    ... ]
    >>> create_batch(people=people_to_create)
    """
    people_batch = {"contacts": people}
    validate(people_batch, PERSON_BATCH)
    people_batch.update({"readMask": ",".join(fields)})
    return _client().people().batchCreateContacts(body=people_batch).execute()
