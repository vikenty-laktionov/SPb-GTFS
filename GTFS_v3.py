import pandas as pd
import time
import requests
import zipfile
import os

#загружаем GTFS feed
if not os.path.exists('feed.zip'):
    url = 'http://transport.orgp.spb.ru/Portal/transport/internalapi/gtfs/feed.zip'
    print(f'Downloading GTFS feed')
    r = requests.get(url, allow_redirects=True, verify='russian_trusted_root_ca.cer')
    print(f'Download complete')
    open('feed.zip', 'wb').write(r.content)
with zipfile.ZipFile('feed.zip', 'r') as zip_ref:
     zip_ref.extract('calendar.txt')
     zip_ref.extract('trips.txt')
     zip_ref.extract('routes.txt')

#грузим датафреймы
trips_file = 'trips.txt'
trips_df = pd.read_csv(trips_file, index_col='trip_id')
services_file = 'calendar.txt'
services_df = pd.read_csv(services_file, index_col='service_id')
routes_file = 'routes.txt'
routes_df = pd.read_csv(routes_file, index_col='route_id')

#считаем рейсы
trips_df = trips_df.join(services_df[['monday','saturday']], on='service_id')
routes_df = routes_df[['route_short_name','transport_type']]
routes_df = routes_df.join(trips_df[trips_df.direction_id==0].groupby('route_id')[['monday','saturday']].sum())
routes_df = routes_df.join(trips_df[trips_df.direction_id==1].groupby('route_id')[['monday','saturday']].sum(), rsuffix='_return')
routes_df = routes_df.fillna(0)
routes_df['monday_total'] = routes_df['monday'] + routes_df['monday_return']
routes_df['saturday_total'] = routes_df['saturday'] + routes_df['saturday_return']

#выводим в файл
timestr = time.strftime("%Y%m%d")
routes_df.to_csv('result'+timestr+'.csv', index=False)

#чистим загруженные файлы
os.remove('calendar.txt')
os.remove('trips.txt')
os.remove('routes.txt')
os.remove('feed.zip')