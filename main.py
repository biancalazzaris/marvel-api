import requests
import datetime as dt
import hashlib 
import pandas as pd
from key import PUBLIC_KEY, PRIVATE_KEY
from pyspark.sql import SparkSession
from pyspark.sql.types import *

timestamp = dt.datetime.now().strftime('%Y-%m-%d%H:%M:%S')

class marvel_series():

    def __init__(self):
        self.base_endpoint = "https://gateway.marvel.com:443/v1/public/series"

    def params_api(self):        
        hash_md5 = hashlib.md5()
        hash_md5.update(f'{timestamp}{PRIVATE_KEY}{PUBLIC_KEY}'.encode('utf-8'))
        hashed_params = hash_md5.hexdigest()

        return hashed_params
            
    def get_data(self):
        endpoint = self.base_endpoint
        params = {'ts': timestamp, 'apikey': PUBLIC_KEY, 'hash': self.params_api(), 'offset': 0, 'limit':100}
        response = requests.get(endpoint,  params=params)
        return response.json()




objeto = marvel_series()
print(objeto.get_data(0))



#  """ Construindo o Data Frame """
# table_name = 'series'
# results = pd.DataFrame(response["data"]["results"])
# # results.to_json('series.json')
# spark = SparkSession.builder.appName("demo-app").getOrCreate()
# df = spark.read.json("./series.json")
# df.createOrReplaceTempView(table_name)
# df_sql = spark.sql("SELECT * FROM series").show()
# # df.show()


# """Conectando com o banco MYSQL"""
# engine = create_engine('mysql+://root:password@localhost:3306/db') #change to connect your mysql
# #if you want to append the data to an existing table
# # df.to_sql(name='SQL Table name',con=engine,if_exists='append',index=False) 

# #if you want to create a new table 
# df.to_sql(name='series',con=engine,if_exists='fail',index=False) 
