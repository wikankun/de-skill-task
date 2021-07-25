import requests
import pandas as pd

url = 'https://stein.efishery.com/v1/storages/5e1edf521073e315924ceab4/list'
response = requests.get(url)
data = response.json()

# Week | Komoditas | Average Price | Min Price | Max Price
df = pd.DataFrame(data)

df1 = df.dropna()
df1 = df1[df1['price'].map(len) > 2]
df1['komoditas'] = df1['komoditas'].str.lower().str.replace(r'(ikan)', '').str.strip()
df1 = df1[df1['komoditas'] != '']
df1 = df1[~df1['komoditas'].str.contains('|'.join(['app', 'test', 'komo', 'indosiar', 'duyung']))]

price = pd.to_numeric(df1['price'])

date_iso = pd.to_datetime(df1['tgl_parsed'], utc=True)
week = pd.to_datetime(date_iso).dt.strftime('%Y-%V')

new_df = pd.DataFrame(df1[['komoditas']])
new_df['price'] = price
new_df['week'] = week

aggregated_df = pd.DataFrame(new_df.groupby(['week', 'komoditas']).mean())
aggregated_df = aggregated_df.rename(columns={'price': 'average_price'})

min_price = new_df.groupby(['week', 'komoditas']).min()
max_price = new_df.groupby(['week', 'komoditas']).max()

aggregated_df['min_price'] = min_price
aggregated_df['max_price'] = max_price

aggregated_df.to_csv('aggregated_df.csv')

