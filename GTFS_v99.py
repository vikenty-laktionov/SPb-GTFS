import pandas as pd
import time
import requests
import zipfile
import os

#загружаем GTFS feed
#if not os.path.exists('feed.zip'):
url = 'http://transport.orgp.spb.ru/Portal/transport/internalapi/gtfs/feed.zip'
print(f'Downloading GTFS feed')
r = requests.get(url, allow_redirects=True, verify='russian_trusted_root_ca.cer')
print(f'Download complete')
open('feed.zip', 'wb').write(r.content)
with zipfile.ZipFile('feed.zip', 'r') as zip_ref:
     zip_ref.extract('calendar.txt')
     zip_ref.extract('trips.txt')
     zip_ref.extract('routes.txt')
     zip_ref.extract('stop_times.txt')

#грузим датафреймы
trips_file = 'trips.txt'
trips_df = pd.read_csv(trips_file, index_col='trip_id')
services_file = 'calendar.txt'
services_df = pd.read_csv(services_file, index_col='service_id')
routes_file = 'routes.txt'
routes_df = pd.read_csv(routes_file, index_col='route_id')
timetables_file = 'stop_times.txt'
timetables_df = pd.read_csv(timetables_file)
routedata_file = 'routedata.csv'
routedata_df = pd.read_csv(routedata_file, index_col='route_id')

#считаем рейсы
trips_df = trips_df.join(services_df[['monday','saturday']], on='service_id')
routes_df = routes_df[['route_short_name','transport_type']]
routes_df = routes_df.join(trips_df[trips_df.direction_id==0].groupby('route_id')[['monday','saturday']].sum())
routes_df = routes_df.join(trips_df[trips_df.direction_id==1].groupby('route_id')[['monday','saturday']].sum(), rsuffix='_return')
routes_df = routes_df.fillna(0)
routes_df['monday_total'] = routes_df['monday'] + routes_df['monday_return']
routes_df['saturday_total'] = routes_df['saturday'] + routes_df['saturday_return']

#всякое про интервалы и пробег
def get_time(timestring):
    return int(timestring[0:2])*60 + int(timestring[3:5])
timetables_df['arr_time'] = timetables_df['arrival_time'].apply(get_time)
timetables_df['dep_time'] = timetables_df['departure_time'].apply(get_time)
trips_df = trips_df.join(timetables_df.groupby('trip_id').shape_dist_traveled.max())
trips_df = trips_df.join(timetables_df.groupby('trip_id').dep_time.min())
trips_df = trips_df.join(timetables_df.groupby('trip_id').apply(lambda x: x.arr_time.max()-x.dep_time.min()).to_frame())
trips_df.columns.values[8]='probeg'
trips_df['avg_speed'] = (trips_df.shape_dist_traveled/trips_df.probeg*60).round(1)
trips_df = trips_df.sort_values('dep_time')
trips_df['interval'] = trips_df.groupby(['route_id','direction_id','monday'])['dep_time'].diff()
routes_df = routes_df.join(trips_df[trips_df.monday==1].groupby('route_id')['avg_speed'].mean())
routes_df = routes_df.join(trips_df[trips_df.saturday==1].groupby('route_id')['avg_speed'].mean(), rsuffix='_sat')
routes_df = routes_df.join(trips_df[(trips_df.direction_id==0)&(trips_df.monday==1)]
 .groupby('route_id')['interval']
 .apply(lambda x: x.value_counts().first_valid_index()), on='route_id', rsuffix='vasek')
routes_df = routes_df.join(trips_df[(trips_df.direction_id==0)&(trips_df.monday==1)]
 .groupby('route_id')['interval']
 .apply(lambda x: x.value_counts().max()), on='route_id', rsuffix='_count')
routes_df = routes_df.join(trips_df[(trips_df.direction_id==0)&(trips_df.monday==1)&(trips_df.dep_time<600)]
 .groupby('route_id')['interval'].mean(), on='route_id', rsuffix='_morning')
routes_df = routes_df.join(trips_df[(trips_df.direction_id==0)&(trips_df.monday==1)&(trips_df.dep_time>599)&(trips_df.dep_time<960)]
 .groupby('route_id')['interval'].mean(), on='route_id', rsuffix='_day')
routes_df = routes_df.join(trips_df[(trips_df.direction_id==0)&(trips_df.monday==1)&(trips_df.dep_time>959)&(trips_df.dep_time<1200)]
 .groupby('route_id')['interval'].mean(), on='route_id', rsuffix='_evening')
routes_df = routes_df.join(trips_df[(trips_df.direction_id==0)&(trips_df.monday==1)&(trips_df.dep_time>1199)]
 .groupby('route_id')['interval'].mean(), on='route_id', rsuffix='_night')
routes_df = routes_df.join(trips_df[(trips_df.direction_id==0)&(trips_df.monday==1)]
 .groupby('route_id')['interval'].max(), on='route_id', rsuffix='_max')
routes_df = routes_df.join(trips_df[(trips_df.direction_id==0)&(trips_df.saturday==1)&(trips_df.dep_time<600)]
.groupby('route_id')['interval'].mean(), on='route_id', rsuffix='_satmorning')
routes_df = routes_df.join(trips_df[(trips_df.direction_id==0)&(trips_df.saturday==1)&(trips_df.dep_time>599)&(trips_df.dep_time<960)]
.groupby('route_id')['interval'].mean(), on='route_id', rsuffix='_satday')
routes_df = routes_df.join(trips_df[(trips_df.direction_id==0)&(trips_df.saturday==1)&(trips_df.dep_time>959)&(trips_df.dep_time<1200)]
.groupby('route_id')['interval'].mean(), on='route_id', rsuffix='_satevening')
routes_df = routes_df.join(trips_df[(trips_df.direction_id==0)&(trips_df.saturday==1)&(trips_df.dep_time>1199)]
.groupby('route_id')['interval'].mean(), on='route_id', rsuffix='_satnight')
routes_df = routes_df.join(trips_df[(trips_df.direction_id==0)&(trips_df.saturday==1)]
 .groupby('route_id')['interval'].max(), on='route_id', rsuffix='_satmax')
routes_df = routes_df.join(trips_df[(trips_df.direction_id==0)&(trips_df.monday==1)]
.groupby('route_id')['probeg'].mean(), on='route_id')
routes_df = routes_df.join(trips_df[(trips_df.direction_id==1)&(trips_df.monday==1)]
.groupby('route_id')['probeg'].mean(), on='route_id', rsuffix='_rt')
routes_df = routes_df.join(trips_df[(trips_df.direction_id==0)&(trips_df.monday==1)]
.groupby('route_id')['shape_dist_traveled'].max(), on='route_id')
routes_df = routes_df.join(trips_df[(trips_df.direction_id==1)&(trips_df.monday==1)]
.groupby('route_id')['shape_dist_traveled'].max(), on='route_id', rsuffix='_rt')
routes_df['shape_dist_traveled'].fillna(0, inplace=True)
routes_df['shape_dist_traveled_rt'].fillna(0, inplace=True)
routes_df['probeg'].fillna(0, inplace=True)
routes_df['probeg_rt'].fillna(0, inplace=True)
routes_df['km_day'] = routes_df['monday']*routes_df['shape_dist_traveled'] + routes_df['monday_return']*routes_df['shape_dist_traveled_rt']
routes_df['hr_day'] = (routes_df['monday']*routes_df['probeg'] + routes_df['monday_return']*routes_df['probeg_rt'])/60

#добавляем табличные данные
routes_df = routes_df.join(routedata_df)

#выводим в файл
timestr = time.strftime("%Y%m%d")
routes_df.round(2).to_csv('DB'+timestr+'.csv')

#чистим загруженные файлы
os.remove('calendar.txt')
os.remove('trips.txt')
os.remove('routes.txt')
os.remove('feed.zip')
os.remove('stop_times.txt')