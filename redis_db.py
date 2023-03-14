import redis
import pandas as pd


class RedisDB:
    def __init__(self, host, port):
        self.pool = redis.ConnectionPool(host=host, port=port, encoding='utf-8', db=0)
        self.db = redis.Redis(connection_pool=self.pool)

    def delete_data(self):
        keys = self.db.keys('*')
        self.db.delete(*keys)

    def insert_imgw_to_redis(self, data):
        names = data['station_code']
        # data['value'] = [nam.replace(',', '.') for nam in data['value']]
        index = data.index
        for i in index:
            test = data[['parameter', 'date', 'value']].loc[[i]]
            rest = test.values.tolist()
            value = (' '.join([str(e) for e in rest[0]])).replace(' ', ',')
            self.db.hset('imgw', str(names[i]) + f'_{i}', value)

    def insert_data_to_redis(self, data):
        data.reset_index(inplace=True)
        names = data['id_localid']
        data.loc[:,'name'] = [nam.replace(' ', '_') for nam in data.loc[:, 'name']]
        data['powiat'] = [nam.replace(' ', '_') for nam in data['powiat']]

        '''insert do bazy'''
        for i in range(len(names)):
            test = data[['name', 'x', 'y', 'powiat', 'wojewodztwo']].loc[[i]]
            rest = test.values.tolist()
            value = (' '.join([str(e) for e in rest[0]])).replace(' ', ',')
            self.db.hset('station', str(names[i]), value)

    def get_data_from_redis(self):
        all_keys = [key.decode("utf-8") for key in self.db.hkeys('station')]
        dataframe = pd.DataFrame(columns=['id_localid', 'name', 'x', 'y', 'powiat', 'wojewodztwo'])
        for key in all_keys:
            try:
                result = self.db.hget('station', key).decode("utf-8").split(',')
                result.insert(0, key)
                dataframe.loc[len(dataframe)] = result
            except Exception as e:
                print("error for id {}: {}".format(key, e))
        dataframe.id_localid.astype(int)
        return dataframe.set_index('id_localid')

    def get_imgw_from_redis(self):
        all_keys = [key.decode("utf-8") for key in self.db.hkeys('imgw')]
        dataframe = pd.DataFrame(columns=['station_code', 'parameter', 'date1', 'hour', 'value'])
        for key in all_keys:
            try:
                result = self.db.hget('imgw', key).decode("utf-8").split(',')
                # result=result[1:5]
                result.insert(0, key)
                dataframe.loc[len(dataframe)] = result
            except Exception as e:
                print("error for id {}: {}".format(key, e))
        dataframe["date"] = dataframe['date1'] + " " + dataframe["hour"]
        df = dataframe.loc[:, ['station_code', 'parameter', 'date', 'value']]
        df['station_code'] = [nam.split('_')[0] for nam in df['station_code']]
        # not tested
        # df.loc[:, 'name'] = [nam.replace(' ', '_') for nam in df.loc[:, 'name']]
        # df['powiat'] = [nam.replace(' ', '_') for nam in df['powiat']]
        #--
        df = df.sort_values(by=['station_code', 'date'])
        # df.station_code = df.station_code.astype(int)
        df.value = df.value.astype(float)

        return df


if __name__ == '__main__':
    redis_data = RedisDB(host='127.0.0.1', port=6379)
    # redis_data.delete_data()
    # temp = pd.read_csv('test.csv', index_col=False, sep=';', names=['station_code', 'parameter', 'date', 'value'])
    # redis_data.insert_imgw_to_redis(temp)
    # redis_data.insert_data_to_redis(pd.read_csv('ready_codes.csv'))
    down_data = redis_data.get_imgw_from_redis()
    print()
    print(type(down_data['value'][0]))
    # stations = redis_data.get_data_from_redis()
    # print(stations)