# se importan las librerias argparse y ConfigParser para recibir parametros en las funciones
import argparse, ConfigParser
# se importan las librerias os y sys para enviar comandos al sistema operativo.
import os, sys
# se importa la libreria traceback para manejo de errores y excepciones
import traceback
# se importa la libreria time para realiza un sleep mientras que spark finaliza la consolidacion de los archivos
import time
# Libreria para la generacion de las fechas
import datetime

# Se importa la libreria pyspark las funciones para declarar el contexto (SparkContext) y definir la configuracion (SparkConf)
from pyspark import SparkContext, SparkConf
# Se importa la libreria pyspark.sql la funciona para definir el contexto de una consulta (SQLContext)
from pyspark.sql import SQLContext
# Se importa la libreria pyspark.sql.types todas las funciones de los tipos de datos en las consultas
from pyspark.sql.types import *

def funcion_order_details_options(app_args):
    try:
        # asigna a la variable con el nombre de la aplicacion que se ejecutara en spark, le pasa como parametro el pais
        conf = SparkConf().setAppName("reshift_order_details_options_by_"+app_args["pais"]+"_to_hdfs")
		# se crea la variable sc que defibne el contexto de ejecucion con el parametro de configuracion
        sc = SparkContext(conf=conf)
		# se crea la variable sqlContext para realizar la consulta
        sqlContext = SQLContext(sc)

		# se realiza la conexion a peyabi sobre la vista vw_orders_details_options_to_hdfs
        dataframe_postgresql_orders_details_options = sqlContext.read.format("jdbc").option("url", "jdbc:postgresql://redshift-bi.peya.co:5439/peyabi").option("dbtable", "temporary.orders_details_options_to_hdfs").option("user", "bigdata_etl").option("password", "zej4uoI9YR2m0Nxh84wi").option("useSSL", "false").load()
		# con el dataframe_postgresql_dim area se crea la tabla temporal de spark orders_details_options_to_hdfs
        dataframe_postgresql_orders_details_options.createOrReplaceTempView("order_details_options")

        #Filtro por countries
        data = sqlContext.read.parquet('hdfs://hadoop-namenode:9000/common/countries/')
        data.registerTempTable('countries')
        #Parametro de Pais, si selecciona ALL carga todos las Ordenes, Detalles y Opciones por pais
        if app_args["pais"] == 'all':
            sql_countries = "SELECT country_id, country_name, country_short_name  FROM countries WHERE active "
        else:
            sql_countries = "SELECT country_id, country_name, country_short_name  FROM countries WHERE country_short_name = '" + app_args["pais"] + "'"

        #Obtiene el nombre corto por pais
        countries = sqlContext.sql(sql_countries)
        #Convierte el Dataframe en List
        to_list = [list(row) for row in countries.collect()]

        #Genera por dias las fechas para ir generando y cargando en HDFS
        start = app_args["from_date"]
        end = app_args["to_date"]
        print "Generating dates -> from:" + str(start) + " , to: " + str(end)

        date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end - start).days + 1)]

        for country in to_list:
            print country[1].encode("utf-8", "replace")

            for date in date_generated:

                sqlDF_by_country = sqlContext.sql("SELECT * FROM order_details_options WHERE order_details_options.country_id = " + str(country[0]) + " and hdfs_date = '" + date.strftime("%Y-%m-%d") + "'")
                #print "SELECT * FROM order_details_options WHERE order_details_options.country_id = " + str(country[0]) + " and hdfs_date = '" + date.strftime("%Y-%m-%d") + "'"
                # se elimina el contenido anterior en hdfs de orders_details_options por pais
                os.system('hdfs dfs -rm -f -R /'+country[2]+'/order_details_options/'+date.strftime("%Y-%m-%d")+'/')

                # escribe en hadoop el resultado del join en la carpeta del pais
                sqlDF_by_country.write.parquet('hdfs://hadoop-namenode:9000/'+country[2]+'/order_details_options/'+date.strftime("%Y-%m-%d")+'/', mode='overwrite', compression='snappy')

                print 'Inserted Data /'+country[2]+'/order_details_options/'+date.strftime("%Y-%m-%d")+'/'

    except:
        print traceback.format_exc()
    time.sleep(1) #workaround para el bug del thread shutdown

#Valida formato de fechas de parametros
def mkdate(datestr):
    try:
        return datetime.datetime.strptime(datestr, "%d-%m-%Y")
    except ValueError:
        raise argparse.ArgumentTypeError(datestr + ' is not a proper date string')

def get_app_args():
    # Parametros: Pais, Fecha desde, Fecha Hasta
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--pais", required=True, help="pais: uy, cl, ar, br, pa, py, bo, co")
    parser.add_argument("-fd", "--from_date", required=True, type=mkdate, help="From Date, format: %d-%m-%Y")
    parser.add_argument("-td", "--to_date", required=True, type=mkdate, help="To Date, format: %d-%m-%Y")
    return vars(parser.parse_args())

if __name__ == '__main__':
    app_args = get_app_args()
    funcion_order_details_options(app_args)