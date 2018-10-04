# se importan las librerias argparse y ConfigParser para recibir parametros en las funciones
import argparse, ConfigParser
# se importa la libreria traceback para manejo de errores y excepciones
import traceback
# se importa la libreria time para realiza un sleep mientras que spark finaliza la consolidacion de los archivos
import time

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *

# print_schema
# Retorna el schema de una location pasada por parametros al script (ejemplo /common/countries/)
# Creado: 14/09/2018
# Carlos A. Jaime

def funcion_print_schema(app_args):
    try:

        conf = SparkConf().setAppName("print_schema_")
        sc = SparkContext(conf=conf)
        sqlContext = SQLContext(sc)

        location_schema = 'hdfs://hadoop-namenode:9000'+app_args.location+'/*.parquet'

        print 'Schema: ' + location_schema

        df_describe = sqlContext.read.parquet(location_schema)
        df_describe.printSchema()

    except:
        print traceback.format_exc()
    time.sleep(1)  # workaround para el bug del thread shutdown


def get_app_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--location", help="schema: location on HDFS")
    return parser.parse_args()

if __name__ == '__main__':
    app_args = get_app_args()
    funcion_print_schema(app_args)
