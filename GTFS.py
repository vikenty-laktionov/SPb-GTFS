import csv
import time
import requests
import zipfile
import os

if not os.path.exists('feed.zip'):
    url = 'http://transport.orgp.spb.ru/Portal/transport/internalapi/gtfs/feed.zip'
    r = requests.get(url, allow_redirects=True)
    open('feed.zip', 'wb').write(r.content)

with zipfile.ZipFile('feed.zip', 'r') as zip_ref:
     zip_ref.extract('calendar.txt')
     zip_ref.extract('trips.txt')
     zip_ref.extract('routes.txt')

with open('calendar.txt') as calendar_file:
    csv_reader = csv.reader(calendar_file, delimiter=',')
    line_count = 0
    services = set()
    for row in csv_reader:
        if line_count == 0:
            line_count += 1
        else:
            if row[1] == '1':
                services.add(row[0])
            line_count += 1
    print(f'Processed {line_count} calendar lines.')

with open('trips.txt') as trip_file:
    csv_reader = csv.reader(trip_file, delimiter=',')
    line_count = 0
    routes = set()
    for row in csv_reader:
        if line_count == 0:
            line_count += 1
        else:
            routes.add(row[0])
            line_count += 1
    print(f'Processed {line_count} route lines.')

with open('trips.txt') as trip_file:
    csv_reader = csv.reader(trip_file, delimiter=',')
    tripcounterforward = dict.fromkeys(routes, 0)
    tripcounterbackward = dict.fromkeys(routes, 0)
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
            line_count += 1
        else:
            if row[1] in services:
                if row[3] == '1':
                    tripcounterforward[row[0]] += 1
                else:
                    tripcounterbackward[row[0]] += 1
            line_count += 1

timestr = time.strftime("%Y%m%d")
with open('routes.txt', encoding='utf-8') as route_file, open('result'+timestr+'.csv', mode='w', newline='', encoding='utf-8') as result_file:
    csv_reader = csv.reader(route_file, delimiter=',')
    csv_writer = csv.writer(result_file, delimiter=',')
    routenames = {}
    line_count = 0
    write_line_count = 0
    for row in csv_reader:
        if line_count == 0:
            csv_writer.writerow(['Маршрут','Вид транспорта','туда','обратно','всего'])
            line_count += 1
        else:
            routenames[row[0]]=[row[2],row[5]]
            line_count += 1
    print(f'Processed {line_count} routename lines.')
    for routename in routenames:
        if routename in tripcounterforward and routenames[routename][0][0] != 'К':
            csv_writer.writerow([routenames[routename][0], routenames[routename][1], tripcounterforward[routename], tripcounterbackward[routename], int(tripcounterforward[routename])+int(tripcounterbackward[routename])])
            write_line_count += 1
    print(f'Wrote {write_line_count} result lines.')

os.remove('calendar.txt')
os.remove('trips.txt')
os.remove('routes.txt')