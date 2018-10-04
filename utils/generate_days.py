# se importan las librerias argparse y ConfigParser para recibir parametros en las funciones
import argparse, ConfigParser
# se importa la libreria traceback para manejo de errores y excepciones
import traceback
# se importa la libreria time para realiza un sleep mientras que spark finaliza la consolidacion de los archivos
import time
# Libreria para la generacion de las fechas
import datetime

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *


# print_schema
# Retorna el schema de una location pasada por parametros al script (ejemplo /common/countries/)
# Creado: 14/09/2018
# Carlos A. Jaime

def funcion_generate_days(app_args):
    try:

        start = app_args["from_date"]
        end = app_args["to_date"]
        print "Generating dates -> from:" + str(start) + " , to: " + str(end)

        date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end - start).days + 1)]

        for date in date_generated:
            print date.strftime("%d-%m-%Y")

    except:
        print traceback.format_exc()
    time.sleep(1)  # workaround para el bug del thread shutdown

def mkdate(datestr):
    try:
        return datetime.datetime.strptime(datestr, "%d-%m-%Y")
    except ValueError:
        raise argparse.ArgumentTypeError(datestr + ' is not a proper date string')

def get_app_args():

    parser = argparse.ArgumentParser()
    parser.add_argument("-fd", "--from_date", required=True, type=mkdate, help="From Date, format: %d-%m-%Y")
    parser.add_argument("-td", "--to_date", required=True, type=mkdate, help="To Date, format: %d-%m-%Y")
    return vars(parser.parse_args())

if __name__ == '__main__':
    app_args = get_app_args()
    funcion_generate_days(app_args)
