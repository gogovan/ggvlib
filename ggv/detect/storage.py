from google.cloud import storage
from ggv.log import logger
import pandas as pd
import os


def upload_df(df: pd.DataFrame, path: str) -> None:
    b = os.environ["BUCKET"]
    logger.info(f"Loading bucket: {b}")
    client = storage.Client()
    bucket = client.bucket(b)
    bucket.blob(path).upload_from_string(df.to_csv(index=False), "text/csv")
    logger.info(f"df -> gs://{b}/{path}")
