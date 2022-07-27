from datetime import datetime, timedelta
from ggv.detect import process_cheating_upload


date = str(datetime.utcnow().date() - timedelta(days=1))

process_cheating_upload(date=date, country="sg")
