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

def funcion_restaurants(app_args):
    try:
        # asigna a la variable con el nombre de la aplicacion que se ejecutara en spark, le pasa como parametro el pais
        conf = SparkConf().setAppName("reshift_dim_restaurant_by_"+app_args.pais+"_to_hdfs")
		# se crea la variable sc que defibne el contexto de ejecucion con el parametro de configuracion
        sc = SparkContext(conf=conf)
		# se crea la variable sqlContext para realizar la consulta
        sqlContext = SQLContext(sc)


        #Filtro por countries
        data = sqlContext.read.parquet('hdfs://hadoop-namenode:9000/common/countries/')
        data.registerTempTable('countries')
        #Parametro de Pais, si selecciona ALL carga todos los restaurantes por pais
        if app_args.pais == 'all':
            sql_countries = "SELECT country_id, country_name, country_short_name FROM countries WHERE active "
        else:
            sql_countries = "SELECT country_id, country_name, country_short_name FROM countries WHERE country_short_name = '" + app_args.pais + "'"

        countries = sqlContext.sql(sql_countries)
        #countries.show()

        to_list = [list(row) for row in countries.collect()]

        for rows in to_list:
            print rows[1].encode("utf-8", "replace")

    except:
        print traceback.format_exc()
    time.sleep(1) #workaround para el bug del thread shutdown

def get_app_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--pais", help="pais: uy, cl, ar, br, pa, py, bo, co")
    return parser.parse_args()

if __name__ == '__main__':
    app_args = get_app_args()
    funcion_restaurants(app_args)