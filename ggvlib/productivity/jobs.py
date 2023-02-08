import json
import enum
from ggvlib.google import pubsub


class JobStatus(enum.Enum):
    ran = 1
    failed = 2
    succeeded = 3


def publish_job_status(
    job_result: JobStatus,
    data: dict[str, str],
    attributes: dict[str, str],
    project_id: str,
    topic_id: str,
):
    """A helper function to publish the status of a Google Cloud Function

    Args:
        job_result (JobStatus): The result of the function
        data (dict[str, str]): The data to publish
        attributes (dict[str, str]): Any attributes to publish
        project_id (str): The project id
        topic_id (str): The topic to publish to
    """
    attributes.update({"result": job_result.name})
    pubsub.publish_to_topic(
        json.dumps(data),
        project_id=project_id,
        topic_id=topic_id,
        attributes=attributes,
    )
