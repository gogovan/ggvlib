from datetime import datetime, timedelta
import os
from ggv.detect import process_cheating_upload
from ggv.detect.storage import upload_df


def run_driver_cheat_detection(message: dict = {}, event: dict = {}) -> None:
    # countries = os.environ["COUNTRIES"].split("|")
    countries = ['hk','sg','vn']

    # base = datetime.utcnow().date()-timedelta(days = 99)
    # date_list = [str(base + timedelta(days=x*4)) for x in range(20)]
    # date_list = ['2022-09-21','2022-09-25','2022-09-29','2022-10-03','2022-10-07']
    # for yesterday in date_list:
    # yesterday = str(datetime.utcnow().date() - timedelta(days=1))
    yesterday = '2022-09-17'
    for c in countries:
        driver_summary, order_output = process_cheating_upload(date=yesterday, country=c)
        print(driver_summary)
        print(order_output)
            # upload_df(
            #     df=order_output,
            #     path=f"import/etl/staging/cheat_detection/order_output/country={c}/date={yesterday}/results.csv",
            # )
            # upload_df(
            #     df=driver_summary,
            #     path=f"import/etl/staging/cheat_detection/driver_summary/country={c}/date={yesterday}/results.csv",
            # )
