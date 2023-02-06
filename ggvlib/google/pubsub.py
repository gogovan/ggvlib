from google.cloud import pubsub_v1
from ggvlib.logging import logger


def publish_to_topic(
    data: str, project_id: str, topic_id: str, attributes: dict[str, str]
) -> None:
    topic = f"projects/{project_id}/topics/{topic_id}"
    publisher = pubsub_v1.PublisherClient()
    publish_future = publisher.publish(
        topic, data.encode("utf-8"), **attributes
    )
    logger.info(f"Published messages to {topic}.")
    return publish_future.result()
