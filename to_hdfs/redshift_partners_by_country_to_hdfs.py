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

def funcion_partners(app_args):
    try:
        # asigna a la variable con el nombre de la aplicacion que se ejecutara en spark, le pasa como parametro el pais
        conf = SparkConf().setAppName("reshift_partners_by_"+app_args.pais+"_to_hdfs")
		# se crea la variable sc que defibne el contexto de ejecucion con el parametro de configuracion
        sc = SparkContext(conf=conf)
		# se crea la variable sqlContext para realizar la consulta
        sqlContext = SQLContext(sc)

		# se realiza la conexion a peyabi sobre la tabla partners
        dataframe_postgresql_partners = sqlContext.read.format("jdbc").option("url", "jdbc:postgresql://redshift-bi.peya.co:5439/peyabi").option("dbtable", "public.vw_restaurant_to_hdfs").option("user", "bigdata_etl").option("password", "zej4uoI9YR2m0Nxh84wi").option("useSSL", "false").load()
		# con el dataframe_postgresql_partners se crea la tabla temporal de spark restaurant
        dataframe_postgresql_partners.createOrReplaceTempView("partners")


        #Prepara la sentencia SQL
        sql = "SELECT * FROM partners WHERE partners.country_id = "

        #Filtro por countries
        data = sqlContext.read.parquet('hdfs://hadoop-namenode:9000/common/countries/')
        data.registerTempTable('countries')
        #Parametro de Pais, si selecciona ALL carga todos los restaurantes por pais
        if app_args.pais == 'all':
            sql_countries = "SELECT country_id, country_name, country_short_name  FROM countries WHERE active "
        else:
            sql_countries = "SELECT country_id, country_name, country_short_name  FROM countries WHERE country_short_name = '" + app_args.pais + "'"

        #Obtiene el nombre corto por pais
        countries = sqlContext.sql(sql_countries)
        #Convierte el Dataframe en List
        to_list = [list(row) for row in countries.collect()]

        for country in to_list:
            print country[1].encode("utf-8", "replace")
            sqlDF_by_country = sqlContext.sql(sql + str(country[0]))
            # se elina el contenido anterior en hdfs de dim_restaurant por pais
            os.system('hdfs dfs -rm -f -R /'+country[2]+'/partners')
            # escribe en hadoop el resultado del join en la carpeta del pais
            sqlDF_by_country.write.parquet('hdfs://hadoop-namenode:9000/'+country[2]+'/partners/', mode='overwrite', compression='snappy')
            print 'Inserted Data /'+country[2]+'/partners'

            #Imprime el schema creado
            df_describe = sqlContext.read.parquet('hdfs://hadoop-namenode:9000/'+country[2]+'/partners/')
            df_describe.printSchema()

    except:
        print traceback.format_exc()
    time.sleep(1) #workaround para el bug del thread shutdown

def get_app_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--pais", help="pais: uy, cl, ar, br, pa, py, bo, co")
    return parser.parse_args()

if __name__ == '__main__':
    app_args = get_app_args()
    funcion_partners(app_args)