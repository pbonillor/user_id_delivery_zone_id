# se importan las librerias os y sys para enviar comandos al sistema operativo.
import os, sys
# se importa la libreria traceback para manejo de errores y excepciones
import traceback
# se importa la libreria time para realiza un sleep mientras que spark finaliza
from time import time
from pyspark import SparkContext, SparkConf
# Se importa la libreria pyspark.sql la funcion para definir el contexto de una consulta (SQLContext)
from pyspark.sql import SQLContext
# Se importa la libreria pyspark.sql.types todas las funciones de los tipos de datos en las consultas
from pyspark.sql.types import *
# Se importa la libreria pyspark.sql la funcion Row
from pyspark.sql import Row
def user(app_args):
        try:
                # asigna a la variable con el nombre de la aplicacion que se ejecutara en spark, le pasa como parametro el pais
                conf = SparkConf().setAppName("user_"+app_args.pais)
                # se crea la variable sc que define el contexto de ejecucion con el parametro de configuracion
                sc = SparkContext(conf=conf)
                # se crea la variable sqlContext para realizar la consulta
                sqlContext = SQLContext(sc)
                df_dim_address = sqlContext.read.parquet('hdfs://hadoop-namenode:9000/'+app_args.pais+'/dim_address/*.parquet')
                df_dim_address.createOrReplaceTempView('dim_address')
		df_address_delivery_zone = sqlContext.read.parquet('hdfs://hadoop-namenode:9000/'+app_args.pais+'/address_delivery_zone/*/*.parquet')
                df_address_delivery_zone.createOrReplaceTempView('address_delivery_zone')
                consulta = 'select address_delivery_zone.partner_id, address_delivery_zone.delivery_zone_id, dim_address.address_id, address_delivery_zone.latitude, address_delivery_zone.longitude, dim_address.user_id from dim_address right join address_delivery_zone on concat(address_delivery_zone.latitude,address_delivery_zone.longitude) = concat(dim_address.latitude,dim_address.longitude)'
                #print consulta
                df_user = sqlContext.sql(consulta)
                os.system('hdfs dfs -rm -f -R /'+app_args.pais+'/user')
                # escribe en hadoop el resultado del join en la carpeta del pais
                df_user.write.parquet('hdfs://hadoop-namenode:9000/'+app_args.pais+'/user/', mode='overwrite', compression='snappy')
        except:
                print traceback.format_exc()
def get_app_args():
        parser = argparse.ArgumentParser()
        parser.add_argument("-p", "--pais", help="primeras dos letras del pais: uy, cl, ar, br, pa, py, bo, co")
        return parser.parse_args()

if __name__ == '__main__':
        app_args = get_app_args()
        user(app_args)
