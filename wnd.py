import tkinter as tk
import customtkinter as ctk
import matplotlib.pyplot as plt
from matplotlib import dates
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from tkcalendar import Calendar
from main_functions import *
from neo import Neo4jDB
import pandas as pd
import mongo_p
import redis_db
import datetime
import time

locations = pd.read_csv("ready_codes.csv").loc[:, ['powiat', 'wojewodztwo']].drop_duplicates()
ctk.set_appearance_mode("Dark")
ctk.set_default_color_theme("blue")
bazy = ['MongoDB', 'Neo4j', 'Redis']
dane = {'hydrologiczne': HYDRO, 'meteorologiczne': METEO}

redis_host = {'host': '127.0.0.1', 'port': 6379}
mongo_host = {'host': 'mongodb://localhost:27017'}
neo_uri = {'uri': "neo4j+s://fb3a78e8.databases.neo4j.io"}

naglowek_style = {'height': 40,
                  'fg_color': ('#0B0B0B', '#22333b'),
                  'corner_radius': 5,
                  'anchor': 'center',
                  'justify': 'center'}
naglowek_dwa_style = {'width': 380,
                      'height': 40,
                      'fg_color': ('#0B0B0B', '#323031'),
                      'corner_radius': 5,
                      'anchor': 'center',
                      'justify': 'center'}


class Wnd(ctk.CTk):
    def __init__(self, title):
        super().__init__()
        self.title(title)
        self.geometry('1000x650')
        self.baza = 'MongoDB'
        self.hydro_meteo = 'hydrologiczne'
        self.powiat = 'raciborski'
        self.wojewodztwo = 'śląskie'
        self.zrodlo_danych = ctk.StringVar(value='link')
        self.czy_zapis = ctk.StringVar(value='NIE')
        self.start_date = datetime.date(2022, 7, 5)
        self.end_date = datetime.date(2020, 7, 15)
        self.rodzaj_danych_ladny = 'Temperatura powietrza'
        self.rodzaj_danych = 'B00300S'
        self.baza_zrodlowa = 'MongoDB'
        self.info = tk.StringVar()
        self._set_window()

    def _set_window(self):
        txt_frame = ctk.CTkFrame(self, width=380)
        txt_frame.pack(side='left', expand=False, padx=15, pady=20, fill='y')
        plot_frame = ctk.CTkFrame(self)
        plot_frame.pack(side='right', expand=True, fill='both', padx=10, pady=20)
        wykresy_title = ctk.CTkLabel(master=plot_frame, text='Wykresy statystyk dla wybranych danych',
                                     font=('Arial', 15, 'bold'), **naglowek_style)
        wykresy_title.pack(side='top', fill='x')
        self.show_parameters(txt_frame)
        self.fig = plt.Figure(figsize=(9, 9), dpi=100)
        self.canvas = FigureCanvasTkAgg(self.fig, plot_frame)

    def calendar_picking(self, name):
        calendar_topLevel = ctk.CTkToplevel(self)
        calendar_topLevel.geometry('300x250')
        calendar_topLevel.title(name)
        cal = Calendar(master=calendar_topLevel)
        cal.pack(pady=10)

        def get_cal_date():
            self.info.set('')
            date = cal.get_date()
            if name == 'Wybierz datę początkową':
                self.start_date = datetime.datetime.strptime(date, '%m/%d/%y')
            else:
                self.end_date = datetime.datetime.strptime(date, '%m/%d/%y')
            calendar_topLevel.destroy()

        start_date_button = ctk.CTkButton(master=calendar_topLevel, text='zatwierdź', command=get_cal_date)
        start_date_button.pack()

    def option_baza_callback(self, choice):
        self.baza = choice

    def option_dane_callback(self, choice):
        self.hydro_meteo = choice
        self.rodzaj_danych_list = list(dane[choice].keys())
        self.rodzaj_danych_option.configure(values=self.rodzaj_danych_list)

    def option_wojewodztwo_callback(self, choice):
        self.wojewodztwo = choice
        self.powiaty = locations.loc[locations['wojewodztwo'] == self.wojewodztwo]['powiat']
        self.powiaty = sorted(self.powiaty, key=str.casefold)
        self.powiaty.insert(0, '-')
        self.powiatoption.configure(values=self.powiaty)
        self.powiatoption.set('powiat')

    def option_powiat_callback(self, choice):
        self.powiat = choice

    def zrodlo_danych_callback(self):
        if self.zrodlo_danych.get() == 'link':
            self.bazy_zr_option.configure(state='disabled')
        else:
            self.bazy_zr_option.configure(state='normal')
            self.czy_zapis.set(value='NIE')
            self.bazyoption.configure(state='disabled')

    def option_baza_zrodlo_callback(self, choice):
        self.baza_zrodlowa = choice

    def option_rodza_dane_callback(self, choice):
        if self.hydro_meteo == 'hydrologiczne':
            self.rodzaj_danych_ladny = choice
            self.rodzaj_danych = HYDRO[choice]
        else:
            self.rodzaj_danych = METEO[choice]
            self.rodzaj_danych_ladny = choice

    def zapis_danych_callback(self):
        if self.czy_zapis.get() == 'TAK':
            self.bazyoption.configure(state='normal')
            self.zrodlo_danych.set(value='link')
            self.bazy_zr_option.configure(state='disabled')
        else:
            self.bazyoption.configure(state='disabled')

    def wywolanie_statystyk(self):
        if self.start_date > self.end_date:
            self.info.set('Nieprawidłowe daty')
            return
        start_time = time.time()
        cpu_start = time.process_time()
        zrodlo = self.zrodlo_danych.get()
        data_type = 'Hydro' if self.hydro_meteo == 'hydrologiczne' else 'Meteo'
        obszar = self.wojewodztwo if self.powiat == '-' else self.powiat
        start_end = {'start': self.start_date, 'end': self.end_date}
        if zrodlo == 'link':
            stations, data = self.from_link(data_type, obszar, start_end)
            if self.czy_zapis.get() == 'TAK':
                if self.baza == 'Redis':
                    self.to_redis(stations, data)
                elif self.baza == 'MongoDB':
                    self.to_mongo(stations, data)
                else:
                    self.to_neo(stations, data)
        else:
            # pobieranie danych z wybranej bazy
            if self.baza_zrodlowa == 'Redis':
                self.from_redis(start_end)
            elif self.baza_zrodlowa == 'MongoDB':
                self.from_mongo(obszar, start_end)
            else:
                self.from_neo(obszar, start_end)
        end_time = time.time()
        elapsed_time = end_time - start_time
        cpu_end = time.process_time()
        cpu_elapsed = cpu_end - cpu_start
        print('Execution time:', time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))
        print('COU Execution time:', time.strftime("%H:%M:%S", time.gmtime(cpu_elapsed)))

    def from_link(self, data_type, obszar, start_end):
        downl_data = download_data(data_type, self.start_date.year, self.start_date.strftime('%m'), self.rodzaj_danych)
        if downl_data.empty:
            self.info.set('Nie można pobrać plików')
            return None, None
        prepared = prepare_data(downl_data)
        all_stations = pd.read_csv("ready_codes.csv").set_index('id_localid')
        f_stat, f_data = filter_data(all_stations, prepared, obszar)
        if len(f_data):
            means, medians = statistic(values=f_data, stations=all_stations, **start_end)
            self.draw_statistics(means, medians)
        else:
            self.info.set('Nie znaleziono danych')
        return f_stat, f_data

    def from_redis(self, start_end):
        redis_data = redis_db.RedisDB(**redis_host)
        down_data = redis_data.get_imgw_from_redis()
        redis_stations = redis_data.get_data_from_redis()
        means_redis, medians_redis = statistic(values=down_data, stations=redis_stations, **start_end)
        self.draw_statistics(means_redis, medians_redis)

    def to_redis(self, stations, data):
        r_data = redis_db.RedisDB(**redis_host)
        r_data.delete_data()
        r_data.insert_data_to_redis(stations)
        r_data.insert_imgw_to_redis(data)

    def from_mongo(self, obszar, start_end):
        mango = mongo_p.MongoDB(**mongo_host)
        mango_stations = mango.get_stations(obszar)
        means_mango, medians_mango = mango_stats(db=mango, stations=mango_stations, **start_end)
        self.draw_statistics(means_mango, medians_mango)

    def to_mongo(self, stations, data):
        mango = mongo_p.MongoDB(**mongo_host)
        mango.delete_data('values')
        mango.delete_data('codes')
        mango.insert_data_codes(stations)
        mango.insert_data_values(data)

    def from_neo(self, obszar, start_end):
        neo_db = Neo4jDB(**neo_uri)
        neo_stations = neo_db.get_stations(obszar)
        means, medians = neo_stats(db=neo_db, parameter=self.rodzaj_danych, stations=neo_stations, **start_end)
        self.draw_statistics(means, medians)
        neo_db.close()

    def to_neo(self, stations, data):
        neo_db = Neo4jDB(**neo_uri)
        neo_db.add_station_data(stations)
        neo_db.add_values(data)
        neo_db.close()

    # funkcja do rysowania wykresu statystyk
    def draw_statistics(self, mean: pd.DataFrame, median: pd.DataFrame):
        self.canvas.get_tk_widget().pack()
        ax1 = self.fig.add_subplot(211)
        ax2 = self.fig.add_subplot(212)
        self.plots('Mean', ax1, mean)
        self.plots('Median', ax2, median)
        self.fig.tight_layout()
        self.canvas.draw()

    def plots(self, title, ax, data):
        ax.clear()
        ax.plot(data.day, label='day', color='gold')
        ax.plot(data.night, label='night', color='mediumblue')
        ax.set_title(f'{self.rodzaj_danych_ladny}: {title}')
        ax.xaxis.set_major_formatter(dates.DateFormatter('%d-%m-%y'))
        ax.grid()
        ax.legend()

    def show_parameters(self, frame):
        parametry_label = ctk.CTkLabel(master=frame, text='Ustaw parametry wejściowe', font=('Arial', 15, 'bold'),
                                       **naglowek_style)
        parametry_label.pack(side='top', fill='x')
        make_label(frame, 'Ustaw czas pomiarów', naglowek_dwa_style)
        calendar_frame = ctk.CTkFrame(master=frame, fg_color="grey17")
        calendar_frame.pack(side='top', expand=False, padx=5, pady=15)
        start_date_button = ctk.CTkButton(master=calendar_frame, text='Wybierz datę poczatkową',
                                          command=lambda v="Wybierz datę początkową": self.calendar_picking(v))
        start_date_button.pack(side='left', padx=10)
        end_date_button = ctk.CTkButton(master=calendar_frame, text='Wybierz datę końcową',
                                        command=lambda v="Wybierz date końcową": self.calendar_picking(v))
        end_date_button.pack(side='right', padx=10)
        make_label(frame, 'Wybierz sposób pobierania danych', naglowek_dwa_style)
        zrodlo_frame = ctk.CTkFrame(master=frame, width=380, fg_color="grey17")
        zrodlo_frame.pack(side='top', expand=False, padx=5, pady=15)
        switch = ctk.CTkSwitch(master=zrodlo_frame, textvariable=self.zrodlo_danych, variable=self.zrodlo_danych,
                               command=self.zrodlo_danych_callback, onvalue='link', offvalue='baza')
        switch.pack(side='left', padx=15)
        self.bazy_zr_option = ctk.CTkOptionMenu(master=zrodlo_frame, values=bazy,
                                                command=self.option_baza_zrodlo_callback, state='disabled')
        self.bazy_zr_option.pack(side='right', padx=15)
        make_label(frame, 'Wybierz czy i do ktorej bazy chcesz zapisywac dane', naglowek_dwa_style)
        zapis_frame = ctk.CTkFrame(master=frame, width=380, fg_color="grey17")
        zapis_frame.pack(side='top', expand=False, padx=5, pady=15)
        switch_zapis = ctk.CTkSwitch(master=zapis_frame, textvariable=self.czy_zapis, variable=self.czy_zapis,
                                     command=self.zapis_danych_callback, onvalue='TAK', offvalue='NIE')
        switch_zapis.pack(side='left', padx=15)
        self.bazyoption = ctk.CTkOptionMenu(master=zapis_frame, values=bazy, command=self.option_baza_callback,
                                            state='disabled')
        self.bazyoption.pack(side='right', padx=15)

        make_label(frame, 'Wybierz rodzaj danych', naglowek_dwa_style)
        dane_frame = ctk.CTkFrame(master=frame, width=380, fg_color="grey17")
        dane_frame.pack(side='top', expand=False, padx=5, pady=15)
        self.rodzaj_danych_list = list(HYDRO.keys())
        self.rodzaj_danych_option = ctk.CTkOptionMenu(master=dane_frame, bg_color="grey17",
                                                      values=self.rodzaj_danych_list,
                                                      command=self.option_rodza_dane_callback, anchor='center')
        self.rodzaj_danych_option.pack(side='right', padx=15, expand=False)
        self.rodzaj_danych_option.set('dane')
        daneoption = ctk.CTkOptionMenu(master=dane_frame, values=['hydrologiczne', 'meteorologiczne'],
                                       command=self.option_dane_callback, anchor='center')
        daneoption.pack(side='left', padx=15, expand=False)
        make_label(frame, 'Wybierz obszar dla którego chcesz liczyć statystyki', naglowek_dwa_style)
        teren_frame = ctk.CTkFrame(master=frame, width=380, fg_color="grey17")
        teren_frame.pack(side='top', expand=False)
        woj_values = sorted(locations['wojewodztwo'].drop_duplicates(), key=str.casefold)
        wojewodztwooption = ctk.CTkOptionMenu(master=teren_frame, values=woj_values,
                                              command=self.option_wojewodztwo_callback, anchor='center')
        wojewodztwooption.set('wojewodztwo')
        wojewodztwooption.pack(side='left', pady=15, padx=15)
        self.powiaty = locations.loc[locations['wojewodztwo'] == self.wojewodztwo]['powiat']
        self.powiaty = pd.concat([self.powiaty, pd.Series(['-'])])
        pow_values = sorted(self.powiaty.values, key=str.casefold)
        self.powiatoption = ctk.CTkOptionMenu(master=teren_frame, values=pow_values,
                                              command=self.option_powiat_callback, anchor='center')
        self.powiatoption.set('powiat')
        self.powiatoption.pack(side='right', pady=15, padx=15)
        make_statistics_button = ctk.CTkButton(master=frame, text='Licz statystyki', command=self.wywolanie_statystyk)
        make_statistics_button.pack(side='top', pady=15)
        label = ctk.CTkLabel(master=frame, textvariable=self.info, **naglowek_dwa_style)
        label.pack(expand=False)


def make_label(wnd, text, style):
    label = ctk.CTkLabel(master=wnd, text=text, wraplength=400, **style)
    label.pack(expand=False)


if __name__ == '__main__':
    window = Wnd('GUI')
    window.mainloop()
