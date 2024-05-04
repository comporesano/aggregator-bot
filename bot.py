from settings import TOKEN
from datetime import datetime, timedelta

date_1 = '2022-09-01T00:00:00'
date_2 = '2023-05-01T00:00:00'

date_1 = datetime.strptime(date_1, '%Y-%m-%dT%H:%M:%S')
date_2 = datetime.strptime(date_2, '%Y-%m-%dT%H:%M:%S')

for i in list(range(date_1.month, (date_2.year - date_1.year) * 12 + date_2.month)):
    if i != 12:
        i %= 12
    print(i)