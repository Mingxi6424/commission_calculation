import requests
import pandas as pd
from datetime import date, timedelta

# 要获取的币种列表
symbols = ['GBP','EUR','CHF','NOK','DKK','SEK','MXN','JPY']

end_date   = date.today()
start_date = end_date - timedelta(days=365)

records = []
for single_date in pd.date_range(start_date, end_date):
    day = single_date.strftime('%Y-%m-%d')
    url = (
        f"https://api.exchangerate.host/{day}"
        f"?base=USD"
        f"&symbols={','.join(symbols)}"
    )
    resp = requests.get(url).json()
    if resp.get('success', True):
        data = {'date': day}
        for cur in symbols:
            data[cur] = resp['rates'].get(cur)
        records.append(data)
    else:
        print(f"❌ {day} 拉取失败")

df_rates = pd.DataFrame(records)
df_rates.to_csv('daily_rates_filtered.csv', index=False)
print(df_rates.tail())
