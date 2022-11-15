import requests
import datetime as dt
import hashlib 
import pandas as pd
from key import PUBLIC_KEY, PRIVATE_KEY
from pyspark.sql import SparkSession
from pyspark.sql.types import *

timestamp = dt.datetime.now().strftime('%Y-%m-%d%H:%M:%S')

class MarvelApi():

    def __init__(self, route: str) -> None:
        self.route = route
        self.base_endpoint = "https://gateway.marvel.com:443/v1/public/"

    def params_api():        
        hash_md5 = hashlib.md5()
        hash_md5.update(f'{timestamp}{PRIVATE_KEY}{PUBLIC_KEY}'.encode('utf-8'))
        hashed_params = hash_md5.hexdigest()

        return hashed_params
            
    def get_data(self, **kwargs):
        endpoint = self.base_endpoint(**kwargs)
        params = {'ts': timestamp, 'apikey': PUBLIC_KEY, 'hash': self.params_api(), 'offset': 0, 'limit':100}
        response = requests.get(endpoint, self.route, params=params)
        return response.json()
        




# for i in range(len(res["data"]["results"])):
#     print(res["data"]["results"][i]["id"],
#         res["data"]["results"][i]["name"])



""" Construindo o Data Frame """
"""
table_name = 'series'
results = pd.DataFrame(response["data"]["results"])
# results.to_json('series.json')

spark = SparkSession.builder.appName("demo-app").getOrCreate()
df = spark.read.json("./series.json")

df.createOrReplaceTempView(table_name)

df_sql = spark.sql("SELECT * FROM series").show()
# df.show()
 """


"""Conectando com o banco MYSQL"""


# engine = create_engine('mysql+://root:password@localhost:3306/db') #change to connect your mysql
# #if you want to append the data to an existing table
# # df.to_sql(name='SQL Table name',con=engine,if_exists='append',index=False) 

# #if you want to create a new table 
# df.to_sql(name='series',con=engine,if_exists='fail',index=False) 


""" try:
    db = connection.connect(host="localhost", database = 'db',user="root", passwd="password",use_pure=True)
    query = "CREATE TABLE IF NOT EXISTS  SERIES"
    df = pd.read_sql(query,db)
    db.close() #close the connection

except Exception as e:
    db.close()
    print(str(e)) """