# se importan las librerias argparse y ConfigParser para recibir parametros en las funciones
import argparse, ConfigParser
# se importan las librerias os y sys para enviar comandos al sistema operativo.
import os, sys
# se importa la libreria traceback para manejo de errores y excepciones
import traceback
# se importa la libreria time para realiza un sleep mientras que spark finaliza la consolidacion de los archivos
import time

# Se importa la libreria pyspark las funciones para declarar el contexto (SparkContext) y definir la configuracion (SparkConf)
from pyspark import SparkContext, SparkConf
# Se importa la libreria pyspark.sql la funciona para definir el contexto de una consulta (SQLContext)
from pyspark.sql import SQLContext
# Se importa la libreria pyspark.sql.types todas las funciones de los tipos de datos en las consultas
from pyspark.sql.types import *


def funcion_dim_address(app_args):
	try:
		# asigna a la variable con el nombre de la aplicacion que se ejecutara en spark, le pasa como parametro el pais
		conf = SparkConf().setAppName("reshift_dim_address_by_"+app_args.pais+"_to_hdfs")
		# se crea la variable sc que defibne el contexto de ejecucion con el parametro de configuracion
		sc = SparkContext(conf=conf)
		# se crea la variable sqlContext para realizar la consulta
		sqlContext = SQLContext(sc)
		# se realiza la conexion a peyabi sobre la tabla dim_address y se asigna a dataframe_postgresql_dim_address
		dataframe_postgresql_dim_address = sqlContext.read.format("jdbc").option("url", "jdbc:postgresql://peyabi.czurab4ej1gp.us-east-1.redshift.amazonaws.com:5439/peyabi").option("dbtable", "public.dim_address").option("user", "bigdata_etl").option("password", "zej4uoI9YR2m0Nxh84wi").option("useSSL", "false").load()
		# se realiza la conexion a peyabi sobre la tabla dim_area y se asigna a dataframe_postgresql_dim_area
		dataframe_postgresql_dim_area = sqlContext.read.format("jdbc").option("url", "jdbc:postgresql://peyabi.czurab4ej1gp.us-east-1.redshift.amazonaws.com:5439/peyabi").option("dbtable", "public.dim_area").option("user", "bigdata_etl").option("password", "zej4uoI9YR2m0Nxh84wi").option("useSSL", "false").load()
		# se realiza la conexion a peyabi sobre la tabla dim_city y se asigna a dataframe_postgresql_dim_city
		dataframe_postgresql_dim_city = sqlContext.read.format("jdbc").option("url", "jdbc:postgresql://peyabi.czurab4ej1gp.us-east-1.redshift.amazonaws.com:5439/peyabi").option("dbtable", "public.dim_city").option("user", "bigdata_etl").option("password", "zej4uoI9YR2m0Nxh84wi").option("useSSL", "false").load()
 		# se realiza la conexion a peyabi sobre la tabla dim_user_address y se asigna a dataframe_postgresql_dim_user_address
                dataframe_postgresql_dim_city = sqlContext.read.format("jdbc").option("url", "jdbc:postgresql://peyabi.czurab4ej1gp.us-east-1.redshift.amazonaws.com:5439/peyabi").option("dbtable", "public.dim_user_address").option("user", "bigdata_etl").option("password", "zej4uoI9YR2m0Nxh84wi").option("useSSL", "false").load()
		# con el dataframe_postgresql_dim_address se crea una tabla temporal de spark dim_address
		dataframe_postgresql_dim_address.createOrReplaceTempView("dim_address")
		# con el dataframe_postgresql_dim area se crea la tabla temporal de spark dim_area
		dataframe_postgresql_dim_area.createOrReplaceTempView("dim_area")
		# con el dataframe_postgresql_dim_city se crea la tabla temporal de spark dim_city
		dataframe_postgresql_dim_city.createOrReplaceTempView("dim_city")
		# con el dataframe_postgresql_dim_user_address se crea la tabla temporal de spark dim_user_address
                dataframe_postgresql_dim_user_address.createOrReplaceTempView("dim_user_address")
		# se realiza la consulta del join para obtener el address_id, latitude y longitude de un pais especifico
		if app_args.pais == 'uy':
			sqlDF_address_by_country = sqlContext.sql("SELECT dim_user_address.user_id AS user_id, dim_address.address_id AS address_id, dim_address.latitude AS latitude, dim_address.longitude AS longitude, dim_address.area_id AS area_id FROM dim_address JOIN dim_user_address on dim_user_address.address_id = dim_address.address_id JOIN dim_area on dim_area.area_id = dim_address.area_id JOIN dim_city on dim_city.city_id = dim_area.city_id where dim_city.country_id = 1 AND dim_address.latitude <> 0.0 AND dim_address.longitude <> 0.0 AND dim_address.latitude IS NOT NULL AND dim_address.longitude IS NOT NULL AND dim_address.latitude<-30.0 AND dim_address.latitude>-35.0 AND dim_address.longitude<-53.0 AND dim_address.longitude>-58.0")
		if app_args.pais == 'cl':
                        sqlDF_address_by_country = sqlContext.sql("SELECT dim_user_address.user_id AS user_id, dim_address.address_id, dim_address.latitude, dim_address.longitude, dim_address.area_id FROM dim_address JOIN dim_user_address on dim_user_address.adress_id = dim_address.address_id JOIN dim_area on dim_area.area_id = dim_address.area_id JOIN dim_city on dim_city.city_id = dim_area.city_id where dim_city.country_id = 2 AND dim_address.latitude <> 0.0 AND dim_address.longitude <> 0.0 AND dim_address.latitude IS NOT NULL AND dim_address.longitude IS NOT NULL AND dim_address.latitude<-17.0 AND dim_address.latitude>-56.0 AND dim_address.longitude<-66.0 AND dim_address.longitude>-75.0")
		if app_args.pais == 'ar':
                        sqlDF_address_by_country = sqlContext.sql("SELECT dim_user_address.user_id AS user_id, dim_address.address_id, dim_address.latitude, dim_address.longitude, dim_address.area_id FROM dim_address JOIN dim_user_address on dim_user_address.address_id = dim_address.address_id JOIN dim_area on dim_area.area_id = dim_address.area_id JOIN dim_city on dim_city.city_id = dim_area.city_id where dim_city.country_id = 3 AND dim_address.latitude <> 0.0 AND dim_address.longitude <> 0.0 AND dim_address.latitude IS NOT NULL AND dim_address.longitude IS NOT NULL AND dim_address.latitude<-22.0 AND dim_address.latitude>-55.0 AND dim_address.longitude<-53.0 AND dim_address.longitude>-74.0")
		if app_args.pais == 'pa':
                        sqlDF_address_by_country = sqlContext.sql("SELECT dim_user_address.user_id AS user_id, dim_address.address_id, dim_address.latitude, dim_address.longitude, dim_address.area_id FROM dim_address JOIN dim_user_address on dim_user_address.address_id = dim_address.address_id JOIN dim_area on dim_area.area_id = dim_address.area_id JOIN dim_city on dim_city.city_id = dim_area.city_id where dim_city.country_id = 11 AND dim_address.latitude <> 0.0 AND dim_address.longitude <> 0.0 AND dim_address.latitude IS NOT NULL AND dim_address.longitude IS NOT NULL")
		if app_args.pais == 'py':
                        sqlDF_address_by_country = sqlContext.sql("SELECT dim_user_address.user_id AS user_id, dim_address.address_id, dim_address.latitude, dim_address.longitude, dim_address.area_id FROM dim_address JOIN dim_user_address on dim_user_address.address_id = dim_address.address_id JOIN dim_area on dim_area.area_id = dim_address.area_id JOIN dim_city on dim_city.city_id = dim_area.city_id where dim_city.country_id = 15 AND dim_address.latitude <> 0.0 AND dim_address.longitude <> 0.0 AND dim_address.latitude IS NOT NULL AND dim_address.longitude IS NOT NULL AND dim_address.latitude<-19.18 AND dim_address.latitude>-27.30 AND dim_address.longitude<-54.19 AND dim_address.longitude>-62.38")
		if app_args.pais == 'bo':
                        sqlDF_address_by_country = sqlContext.sql("SELECT dim_user_address.user_id AS user_id, dim_address.address_id, dim_address.latitude, dim_address.longitude, dim_address.area_id FROM dim_address JOIN dim_user_address on dim_user_address.user_id = dim_address.address_id JOIN dim_area on dim_area.area_id = dim_address.area_id JOIN dim_city on dim_city.city_id = dim_area.city_id where dim_city.country_id = 17 AND dim_address.latitude <> 0.0 AND dim_address.longitude <> 0.0 AND dim_address.latitude IS NOT NULL AND dim_address.longitude IS NOT NULL AND dim_address.latitude<-09.39 AND dim_address.latitude>-22.53 AND dim_address.longitude<-57.25 AND dim_address.longitude>-69.38")
		# se elina el contenido anterior en hdfs de dim_address por pais
		os.system('hdfs dfs -rm -f -R /'+app_args.pais+'/dim_address')
		# escribe en hadoop el resultado del join en la carpeta del pais
		sqlDF_address_by_country.write.parquet('hdfs://hadoop-namenode:9000/'+app_args.pais+'/dim_address/', mode='overwrite', compression='snappy')
	except:
		print traceback.format_exc()
	time.sleep(1) #workaround para el bug del thread shutdown

def get_app_args():
	parser = argparse.ArgumentParser()
	parser.add_argument("-p", "--pais", help="primeras dos letras del pais: uy, cl, ar, br, pa, py, bo, co")
	return parser.parse_args()

if __name__ == '__main__':
	app_args = get_app_args()
	funcion_dim_address(app_args)
