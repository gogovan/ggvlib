from typing import Dict, List
from google.cloud import scheduler_v1
from google.protobuf.json_format import MessageToDict
from google.protobuf import field_mask_pb2
from ggvlib.logging import logger

DEFAULT_JOB = {
    "http_target": {
        "uri": None,
        "http_method": "POST",
        "headers": {
            "Content-Type": "application/json",
            "User-Agent": "Google-Cloud-Scheduler",
        },
        "oidc_token": {
            "service_account_email": None,
            "audience": None,
        },
    },
    "state": "ENABLED",
    "status": {},
    "schedule": None,
    "time_zone": None,
    "retry_config": {
        "max_retry_duration": {"seconds": 0},
        "min_backoff_duration": {"seconds": 5},
        "max_backoff_duration": {"seconds": 360},
        "max_doublings": 5,
    },
    "attempt_deadline": {"seconds": 600},
}


def _parent(project_id: str, location_id: str) -> str:
    return f"projects/{project_id}/locations/{location_id}"


def list(project_id: str, location_id: str) -> List[Dict[str, str]]:
    """Returns a list of all jobs names associated with the project and location.

    Returns:
        List[Dict[str, str]]: A list of job dictionaries

    """
    parent = _parent(project_id, location_id)
    logger.info(f"Fetching all jobs in parent: {parent}")
    request = scheduler_v1.ListJobsRequest(
        parent=parent,
    )
    response = scheduler_v1.CloudSchedulerClient().list_jobs(request)
    responses = []
    all_responses = []
    for page in response.pages:
        responses.append(_proto_to_dict(page._pb).get("jobs"))
    responses = [r for r in responses if r]
    for sublist in responses:
        for item in sublist:
            all_responses.append(item)
    return all_responses


def get(name: str) -> Dict[str, str]:
    """Returns a job dict for a given job name.

    Parameters:
        name (str): The name of the job to fetch

    Returns:
        Dict[str, str]: A job dict

    >>> scheduler.get("projects/sample-gcp-project/locations/asia-east2/jobs/sample-job-name")
    {
        "name": "projects/sample-gcp-project/locations/asia-east2/jobs/sample-job-name",
        "http_target":{"uri":"https://something.a.run.app/run/"}
    }
    """
    request = scheduler_v1.GetJobRequest(
        name=name,
    )
    logger.info(f"Retreiving job: {name}")
    response = scheduler_v1.CloudSchedulerClient().get_job(request=request)
    return _proto_to_dict(response)


def create(name: str, project_id: str, location_id: str, job: Dict[str, str]):
    """Creates a job for a given job dictionary.

    Parameters:
        name (str): The name of the new job to create
        job (Dict[str, str]): The new job

    Returns:
        Dict[str, str]: A job dict
    """
    parent = _parent(project_id, location_id)
    job["name"] = f"{parent}/jobs/{name}"
    logger.info(f"Creating job: {name}")
    return _proto_to_dict(
        scheduler_v1.CloudSchedulerClient().create_job(parent=parent, job=job)
    )


def delete(name: str) -> None:
    """Deletes a job for a given job name.

    Parameters:
        name (str): The name of the job to delete

    Returns:
        None
    """
    logger.info(f"Deleting job: {name}")
    scheduler_v1.CloudSchedulerClient().delete_job(name=name)


def update(update: Dict[str, str]) -> Dict[str, str]:
    """Updates a job for a given job dictionary with only the keys that need to be updated.

    Parameters:
        name (str): The name of the new job to create
        update (Dict[str, str]): The new job information

    Returns:
        Dict[str, str]: An updated job dict
    """
    mask_fields = list(_get_mask_fields(update))
    return _proto_to_dict(
        scheduler_v1.CloudSchedulerClient().update_job(
            job=update,
            update_mask=field_mask_pb2.FieldMask(paths=mask_fields),
        )
    )


def _proto_to_dict(job: scheduler_v1.types.job.Job) -> Dict[str, str]:
    try:
        body = job.http_target.body.decode()
    except Exception as e:
        logger.error(e)
        body = ""
    job_dict = MessageToDict(job._pb)
    if job_dict.get("httpTarget"):
        job_dict["httpTarget"]["body"] = body
    return job_dict


def _get_mask_fields(dic: Dict[str, str], path: str = None):
    if not path:
        path = []
    if isinstance(dic, dict):
        for x in dic.keys():
            local_path = path[:]
            local_path.append(x)
            for b in _get_mask_fields(dic[x], local_path):
                yield b
    else:
        yield ".".join(path)
