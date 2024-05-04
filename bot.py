from settings import TOKEN
from datetime import datetime, timedelta

date_string = '2022-12-01T00:00:00'

date = datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%S')

date_1 = date.replace(year=date.year + 1, month=date.month % 12 + 1) - timedelta(days=1)
print(date_1)