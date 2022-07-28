from datetime import datetime, timedelta
from ggv.detect import process
from ggv.detect.storage import upload_df


def run_driver_cheat_detection(message: dict = {}, event: dict = {}) -> None:
    countries = ["sg", "vn"]

    yesterday = str(datetime.utcnow().date() - timedelta(days=1))

    for c in countries:
        driver_summary, order_output = process(date=yesterday, country=c)
        upload_df(
            df=order_output,
            path=f"import/etl/staging/cheat_detection/order_output/country={c}/date={yesterday}/results.csv",
        )
        upload_df(
            df=driver_summary,
            path=f"import/etl/staging/cheat_detection/driver_summary/country={c}/date={yesterday}/results.csv",
        )
