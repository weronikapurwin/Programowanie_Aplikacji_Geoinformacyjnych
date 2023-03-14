import requests
import tarfile
import zipfile
import io
import pandas as pd
import geopandas as gpd
from astral import LocationInfo
from astral.sun import sun
import datetime
import mongo_p

METEO = {'Temperatura powietrza': 'B00300S',
         "Temperatura gruntu": 'B00305A',
         'Średnia prędkość \nwiatru': 'B00702A',
         'Prędkość maksymalna\nwiatru': 'B00703A',
         'Suma opadu 10 min': 'B00608S',
         'Suma opadu 1h': 'B00606S',
         'Wilgotność względna \npowietrza': 'B00802A',
         'Największy poryw \nw 10min': 'B00714A',
         'Zapas wody w śniegu': 'B00910A'}

HYDRO = {'Stan wody operacyjny': 'B00020S',
         'Przepływ operacyjny': 'B00050S',
         'Stan wody kontrolny': 'B00014A',
         'Temperatura wody': 'B00101A'}


# pobranie wybranych danych Meteo/Hydro dla wybranej daty (rok/miesiąc)
def download_data(d_type: str, year, month, selected: str):
    """
    data type needs to be: Hydro or Meteo and months like Jan need to be 01
    """
    url = f"https://dane.imgw.pl/datastore/getfiledown/Arch/Telemetria/{d_type}/{year}/{d_type}_{year}-{month}"
    tar_request = requests.get(url + ".tar", timeout=30)
    try:
        tar_file = tarfile.open(fileobj=io.BytesIO(tar_request.content))
        return open_tar(tar_file, selected)
    except tarfile.ReadError:
        pass
    zip_request = requests.get(url + ".zip", timeout=30)
    try:
        zip_file = zipfile.ZipFile(io.BytesIO(zip_request.content))
        return open_zip(zip_file, selected)
    except zipfile.BadZipFile:
        return pd.DataFrame()


# otworzenie pobranych plików zip
def open_zip(data, selected):
    file = [n for n in data.filelist if n.filename.startswith(selected)][0]
    with data.open(file, mode="r") as zfile:
        return pd.read_csv(zfile, sep=";", header=None, skipfooter=1, engine="python")


# otworzenie pobranych plików tar
def open_tar(data, selected):
    file = [n for n in data.getmembers() if n.name.startswith(selected)][0]
    with data.extractfile(file) as tfile:
        return pd.read_csv(tfile, sep=";", header=None, skipfooter=1, engine="python")


def TEST_open(csv="B00300S_2022_07.csv"):
    with open(csv, mode="r", encoding="UTF8") as zfile:
        df = pd.read_csv(zfile, sep=";", header=None, skipfooter=1, engine="python")
        return df


# wczytanie danych z pliku i przygotowanie ich do dalszych obliczeń
def prepare_data(data_frame: pd.DataFrame):
    del data_frame[4]
    data_frame.columns = ["station_code", "parameter", "date", "value"]
    if data_frame.loc[:, 'value'].dtype == object:
        data_frame.loc[:, 'value'] = [i.replace(',', '.') for i in data_frame.loc[:, 'value']]
        # data_frame.loc[:,'value'] = data_frame.loc[:,'value'].astype('float')
        data_frame.isetitem(3, data_frame['value'].astype('float'))
    return data_frame


# sprawdza w jakim województwie jest dany powiat
def check_contains(inside, container, col):
    contains = container['geometry'].contains(inside['geometry'].representative_point())
    if True in contains.values:
        result = container.loc[contains, col]
    else:
        distance = container['geometry'].distance(inside['geometry'])
        closest = distance[distance == distance.min()]
        result = container.loc[closest.index, col]
    return result.iloc[0]


def locations_shp():
    powiaty = gpd.read_file('Dane\\powiaty.shp').loc[:, ['name', 'geometry']]
    wojewodztwa = gpd.read_file('Dane\\woj.shp').loc[:, ['name', 'geometry']]
    powiaty['woj'] = powiaty.apply(lambda row: check_contains(row, wojewodztwa, 'name'), axis=1)
    powiaty.rename(columns={'name': 'pow'}, inplace=True)
    powiaty.to_file('Dane\\locations.shp', encoding='utf-8')


# funkcja, kóra zwraca nazwę, współrzędne i info o powiecie i województwie dla stacji
def get_codes(stacje='Dane\\effacility.geojson'):
    df = gpd.read_file(stacje).set_index('id_localid')
    df.index = df.index.astype('int')
    pow_woj = df.apply(lambda row: check_contains(row, locations, ['pow', 'woj']), axis=1)
    df = gpd.GeoDataFrame.set_crs(df, crs=2180, allow_override=True)
    df = df.to_crs(4326)
    names = df.loc[:, 'name1']
    x = df.loc[:, 'geometry'].x
    y = df.loc[:, 'geometry'].y
    result = pd.DataFrame({'name': names, 'x': x, 'y': y, 'powiat': pow_woj['pow'], 'wojewodztwo': pow_woj['woj']})
    result.to_csv('ready_codes.csv')
    return result


# funkcja do obliczenia wschodu i zachodu słońca dla miast w Polsce
def get_sunrise_sunset(dataframe, year, month, day):
    names = dataframe['name']
    x = dataframe['x']
    y = dataframe['y']
    sunset = []
    sunrise = []
    temp = []
    for a, b, name in zip(y, x, names):
        city = LocationInfo(name, 'Poland', 'Europe/Warsaw', a, b)
        s = sun(city.observer, date=datetime.date(year, month, day), tzinfo=city.timezone)
        next_s = sun(city.observer, date=(datetime.date(year, month, day) + datetime.timedelta(days=1)),
                     tzinfo=city.timezone)
        temp.append((next_s['sunrise']))
        sunrise.append(s['sunrise'])
        sunset.append(s['sunset'])
    dataframe['next_sunrise'] = temp
    dataframe['sunrise'] = sunrise
    dataframe['sunset'] = sunset
    return dataframe


# wybieranie stacji i pomiarów na podstawie obszaru
def filter_data(stations, data, obszar):
    jednostka_adm = 'powiat' if obszar in stations.powiat.values else 'wojewodztwo'
    selected = stations.loc[stations.loc[:, jednostka_adm] == obszar]
    ids = selected.index.values.astype('int')
    return selected, data.loc[data.station_code.isin(ids)]


# obliczenie statystyk w przedziale czasu
def statistics_plot(func):
    def wrapper(start, end, **kwargs):
        delta = datetime.timedelta(days=1)
        mean_t = pd.DataFrame(columns=['day', 'night'])
        median_t = pd.DataFrame(columns=['day', 'night'])
        while start <= end:
            date = start.date()
            df = func(rok=start.year, miesiac=start.month, dzien=start.day, **kwargs)
            mean_t.loc[date] = [df.at['day', 'mean'], df.at['night', 'mean'], ]
            median_t.loc[date] = [df.at['day', 'median'], df.at['night', 'median']]
            start += delta
        return mean_t, median_t

    return wrapper


# DLA DANYCH POBRANYCH: obliczenie statystyk dla wybranego dnia i nocy
@statistics_plot
def statistic(values, stations, rok, miesiac, dzien):
    get_sunrise_sunset(stations, rok, miesiac, dzien)
    # select dataframe for 2 days
    current = datetime.date(rok, miesiac, dzien)
    first_date = values[values.date.str.startswith(str(current))]
    next_date = values[values.date.str.startswith(str(current + datetime.timedelta(days=1)))]
    selected = pd.concat([first_date, next_date])

    day = []
    night = []
    for index, row in selected.iterrows():
        kod = row['station_code']
        date = row['date']
        st_data = stations.loc[kod]
        time1 = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M')
        time = time1.astimezone()
        sunrise = st_data.sunrise
        sunset = st_data.sunset
        next_sunrise = st_data.next_sunrise

        if sunrise < time < sunset:
            day.append(row['value'])
        elif sunset < time < next_sunrise:
            night.append(row['value'])
    day = pd.Series(day).agg(["mean", "median"])
    night = pd.Series(night).agg(["mean", "median"])
    return pd.DataFrame([day, night], index=['day', 'night'])


# DLA DANYCH Z NEO4J: obliczenie statystyk dla wybranego dnia i nocy
@statistics_plot
def neo_stats(db, stations, parameter, rok, miesiac, dzien):
    """
    (1.) znalezienie wschodu, zachodu i nast. wchodu
    (2.) zamiana stacji na dict - parametry dla zapytania do bazy
    (3.) obliczenie statystyk: pobranie danych na podstawie nazwy stacji, daty wschodu i zachodu
    """
    get_sunrise_sunset(stations, rok, miesiac, dzien)  # (1.)
    params = stations.to_dict('records')  # (2.)
    day = neo_day_night(db, params, parameter, "sunrise", "sunset")  # (3.)
    night = neo_day_night(db, params, parameter, "sunset", "next_sunrise")
    return pd.DataFrame([day, night], index=['day', 'night'])


# pobranie danych z neo4j i obliczenia
def neo_day_night(db, params, parameter, start, stop):
    values = db.get_values(params, parameter, start, stop)
    return values.value.agg(['mean', 'median'])


@statistics_plot
def mango_stats(db: mongo_p.MongoDB, stations, rok, miesiac, dzien):
    """
    (1.) znalezienie wschodu, zachodu i nast. wchodu
    (2.) zamiana stacji na dict - parametry dla zapytania do bazy
    (3.) obliczenie statystyk: pobranie danych na podstawie nazwy stacji, daty wschodu i zachodu
    """
    get_sunrise_sunset(stations, rok, miesiac, dzien)  # (1.)
    # stations.reset_index(inplace=True)
    params = stations.to_dict('records')  # (2.)
    day = mongo_day_night(db, params, "sunrise", "sunset")  # (3.)
    night = mongo_day_night(db, params, "sunset", "next_sunrise")
    return pd.DataFrame([day, night], index=['day', 'night'])


def mongo_day_night(db, params, start, stop):
    values = db.get_values(params, start, stop)
    # values['value'] = [i.replace(',', '.') for i in values['value']]
    # values['value'] = values['value'].astype('float')
    return values.value.agg(['mean', 'median'])
