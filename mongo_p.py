import pandas as pd
import pymongo


class MongoDB:
    def __init__(self, host):
        self.connection = pymongo.MongoClient(host)
        self.db = self.connection.baza

    def delete_data(self, collection):
        self.db[collection].delete_many({})

    def insert_data_codes(self, data):
        data.reset_index(inplace = True)
        dane = self.db.codes
        df = data
        dane.insert_many(df.to_dict('records'))

    def insert_data_values(self, data):
        dane = self.db.values
        data['date'] = pd.to_datetime(data['date'])
        dane.insert_many(data.to_dict('records'))

    def _get_data(self, powiat_find, no_id=True):
        cursor = self.db['codes'].find({'powiat': powiat_find})
        # Expand the cursor and construct the DataFrame
        df = pd.DataFrame(list(cursor))
        # Delete the _id
        if no_id:
            del df['_id']

        return df

    def get_stations(self, powiat):
        df = self._get_data(powiat)
        df = df[['id_localid', 'name', 'x', 'y']]

        return df

    def get_values(self, params, start, end, no_id=False):
        temp = []
        test=[]
        for i in params:
            start_dla = i[start]
            end_dla = i[end]
            cursor = self.db['values'].find(
                {'date': {"$gte": start_dla, "$lt": end_dla}, 'station_code': i['id_localid']})
            temp.extend(cursor)
        df = pd.DataFrame(temp)
        if no_id:
            del df['_id']
        return df


mango = MongoDB("mongodb://localhost:27017")
# mango.delete_data('values')
# mango.delete_data('codes')
# mango.insert_data_codes(pd.read_csv('ready_codes.csv'))
temp = pd.read_csv('B00300S_2022_07.csv', index_col=False, sep=';',
                   names=['station_code', 'parameter', 'date', 'value'])
print(temp)
