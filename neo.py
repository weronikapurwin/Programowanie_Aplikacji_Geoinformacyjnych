from neo4j import GraphDatabase
import pandas as pd


class Neo4jDB:
    def __init__(self, uri, username="neo4j", password="password"):
        self.driver = GraphDatabase.driver(uri, auth=(username, password))
        self.driver.verify_connectivity()

    def __write_many(self, func):
        with self.driver.session() as session:
            for i in func:
                session.execute_write(*i)

    def close(self):
        self.driver.close()

    def clear(self):
        def detach_delete(tx):
            return tx.run("MATCH (n) DETACH DELETE n")

        with self.driver.session() as session:
            session.execute_write(detach_delete)

    def add_station_data(self, data):
        data.reset_index(inplace=True)
        pow_woj = data.loc[:, ['powiat', 'wojewodztwo']].drop_duplicates()
        self.__write_many(((create_woj, data['wojewodztwo'].unique()),
                           (create_pow, pow_woj.to_dict('records')),
                           (create_station, data.iloc[:, 0:-1].to_dict('records'))))

    def add_values(self, data):
        with self.driver.session() as session:
            session.execute_write(create_values, data.to_dict('records'))

    def get_stations_all(self):
        with self.driver.session() as session:
            result = session.execute_read(read_stations_all)
        df = pd.DataFrame(result, columns=['id_localid', 'name', 'x', 'y', 'powiat', 'wojewodztwo'])
        return df.set_index('id_localid')

    def get_stations(self, name):
        with self.driver.session() as session:
            result = session.execute_read(read_stations, name)
        df = pd.DataFrame(result, columns=['id_localid', 'name', 'x', 'y'])
        return df.set_index('id_localid')

    def get_values(self, data, selected, start, end):
        with self.driver.session() as session:
            result = session.execute_read(read_values, data, selected, start, end)
        return pd.DataFrame(result, columns=['station_code', 'parameter', 'date', 'value'])


def create_woj(tx, data):
    return tx.run("""UNWIND $data as n 
                     MERGE (:Wojewodztwo {name: n})""",
                  data=data)


def create_pow(tx, data):
    return tx.run("""UNWIND $data as n 
                     MATCH (w:Wojewodztwo {name: n.wojewodztwo})
                     MERGE (:Powiat {name: n.powiat})-[:IN]->(w)""",
                  data=data)


def create_station(tx, data):
    return tx.run("""UNWIND $data as n 
                     MATCH (p:Powiat {name: n.powiat})
                     MERGE (:Stacja {name: n.name, x: n.x, y:n.y, localid:n.id_localid})-[:IN]->(p)""",
                  data=data)


def create_values(tx, data):
    return tx.run("""UNWIND $data as n 
                     MATCH (s:Stacja {localid: n.station_code})
                     MERGE (:Pomiar {name:n.parameter, station: n.station_code, date: datetime(replace(n.date, " ", 'T')), value:n.value})-[:MEASURED_AT]->(s)""",
                  data=data)


def read_stations_all(tx):
    result = tx.run("""MATCH (s:Stacja)-->(p:Powiat)-->(w:Wojewodztwo) 
                       RETURN s.localid, s.name, s.x, s.y, p.name, w.name""")
    return result.values('s.localid', 's.name', 's.x', 's.y', 'p.name', 'w.name')


def read_stations(tx, name):
    result = tx.run("""MATCH (s:Stacja)-[:IN*1..2]->(:Powiat|Wojewodztwo{name: $name}) 
                       RETURN s.localid, s.name, s.x, s.y""", name=name)
    return result.values('s.localid', 's.name', 's.x', 's.y')


def read_values(tx, data, selected, start, end):
    result = tx.run("""UNWIND $data as n
                       MATCH (p:Pomiar {name: $selected})-[:MEASURED_AT]->(s:Stacja {name:n.name}) 
                       WHERE p.date >= n[$start] and p.date <= n[$end]
                       RETURN p.station, p.name, p.date, p.value""", data=data, selected=selected, start=start, end=end)
    return result.values('p.station', 'p.name', 'p.date', 'p.value')
