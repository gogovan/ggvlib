from datetime import datetime, timedelta
import os
from ggv.detect import process_cheating_upload
from ggv.detect.storage import upload_df


def run_driver_cheat_detection(message: dict = {}, event: dict = {}) -> None:
    # countries = os.environ["COUNTRIES"].split("|")
    countries = ['hk','vn','sg']

    # base = datetime.utcnow().date()-timedelta(days = 80)
    # date_list = [str(base + timedelta(days=x*4)) for x in range(20)]
    # for yesterday in date_list:
    yesterday = str(datetime.utcnow().date() - timedelta(days=1))
    for c in countries:
        driver_summary, order_output = process_cheating_upload(date=yesterday, country=c)
        upload_df(
            df=order_output,
            path=f"import/etl/staging/cheat_detection/order_output/country={c}/date={yesterday}/results.csv",
        )
        upload_df(
            df=driver_summary,
            path=f"import/etl/staging/cheat_detection/driver_summary/country={c}/date={yesterday}/results.csv",
        )
