from google.cloud import secretmanager
import google_crc32c


def get_value(secret_name: str):
    response = (
        secretmanager.SecretManagerServiceClient().access_secret_version(
            request={"name": secret_name}
        )
    )
    crc32c = google_crc32c.Checksum()
    crc32c.update(response.payload.data)
    if response.payload.data_crc32c != int(crc32c.hexdigest(), 16):
        raise Exception("Data corruption detected in secret payload")
    return response.payload.data.decode("UTF-8")
