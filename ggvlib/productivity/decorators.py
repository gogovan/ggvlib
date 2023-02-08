import json
import base64
import functools
from ggvlib.productivity import jobs


def log_task_execution(project_id: str, topic_id: str, service: str) -> any:
    """Monitor and publish the results of a Cloud Function invocation to PubSub.
    Anything in the 'data' field of the pubsub message will also be published

    Args:
        project_id (str): The Google Project ID
        topic_id (str): The topic to pubish to
        service (str): The name of the service which is being invoked. This will be included in the attribute field

    Returns:
        any: Whatever the decorated function returns
    """

    def log_task_execution_inner(func: callable) -> callable:
        @functools.wraps(func)
        def log_task_execution_wrapper(*args, **kwargs):
            # Publish result as started
            task_context = json.loads(
                base64.b64decode(kwargs["event"]["data"]).decode("utf-8")
            )
            jobs.publish_job_status(
                job_result=jobs.JobStatus(1),
                data=task_context,
                project_id=project_id,
                topic_id=topic_id,
                attributes={"service": service},
            )
            try:
                results = func(*args, **kwargs)
                # Publish result as complete
                jobs.publish_job_status(
                    job_result=jobs.JobStatus(2),
                    data=task_context,
                    project_id=project_id,
                    topic_id=topic_id,
                    attributes={"service": service},
                )
                return results
            except Exception as e:
                # Publish result as failed
                jobs.publish_job_status(
                    job_result=jobs.JobStatus(3),
                    data=task_context,
                    project_id=project_id,
                    topic_id=topic_id,
                    attributes={"service": service},
                )

        return log_task_execution_wrapper

    return log_task_execution_inner
