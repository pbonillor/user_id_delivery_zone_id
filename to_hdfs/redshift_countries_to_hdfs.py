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

# redshift_dim_country_to_hdfs
# Extractor y carga de datos dim_country de PeyaBI DWH a PeyaBI HDFS
# dim_country -> hdfs:/common/
# Creado: 14/09/2018
# Carlos A. Jaime

def funcion_country():
    try:
        # asigna a la variable con el nombre de la aplicacion que se ejecutara en spark
        conf = SparkConf().setAppName("redshift_countries_to_hdfs")
		# se crea la variable sc que defibne el contexto de ejecucion con el parametro de configuracion
        sc = SparkContext(conf=conf)
		# se crea la variable sqlContext para realizar la consulta
        sqlContext = SQLContext(sc)

		# se realiza la conexion a peyabi sobre la tabla dim_country
        dataframe_postgresql_dim_country = sqlContext.read.format("jdbc").option("url", "jdbc:postgresql://peyabi.czurab4ej1gp.us-east-1.redshift.amazonaws.com:5439/peyabi").option("dbtable", "public.dim_country").option("user", "bigdata_etl").option("password", "zej4uoI9YR2m0Nxh84wi").option("useSSL", "false").load()
        # se realiza la conexion a peyabi sobre la tabla dim_country
        dataframe_postgresql_dim_country_details = sqlContext.read.format("jdbc").option("url","jdbc:postgresql://peyabi.czurab4ej1gp.us-east-1.redshift.amazonaws.com:5439/peyabi").option("dbtable", "public.dim_country_details").option("user", "bigdata_etl").option("password","zej4uoI9YR2m0Nxh84wi").option("useSSL", "false").load()

		# con el dataframe_postgresql_se crea temporal de spark dim_country
        dataframe_postgresql_dim_country.createOrReplaceTempView("dim_country")
        # con el dataframe_postgresql_se crea temporal de spark dim_country_details
        dataframe_postgresql_dim_country_details.createOrReplaceTempView("dim_country_details")

        #SQL para la consulta de Countries
        sql_countries = "SELECT dim_country.country_id, dim_country.country_name, dim_country.country_short_name, dim_country.currency_id , dim_country_details.active, dim_country_details.timezone, dim_country_details.matrix_visible FROM dim_country JOIN dim_country_details on dim_country.country_id = dim_country_details.country_id"
        sqlDF_countries = sqlContext.sql(sql_countries)

        # se elina el contenido anterior en hdfs de dim_country
        os.system('hdfs dfs -rm -f -R /common/countries')

        # escribe en hadoop el resultado del join en la carpeta del pais
        sqlDF_countries.write.parquet('hdfs://hadoop-namenode:9000/common/countries/', mode='overwrite', compression='snappy')

        print 'Inserted Data /common/countries'
    except:
        print traceback.format_exc()
    time.sleep(1) #workaround para el bug del thread shutdown

if __name__ == '__main__':
    funcion_country()