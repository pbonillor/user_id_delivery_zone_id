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
def user_csv(app_args):
        try:
                # asigna a la variable con el nombre de la aplicacion que se ejecutara en spark, le pasa como parametro el pais
                conf = SparkConf().setAppName("user_csv_"+app_args.pais)
                # se crea la variable sc que define el contexto de ejecucion con el parametro de configuracion
                sc = SparkContext(conf=conf)
                # se crea la variable sqlContext para realizar la consulta
                sqlContext = SQLContext(sc)
                df_user = sqlContext.read.parquet('hdfs://hadoop-namenode:9000/'+app_args.pais+'/user/*.parquet')
                df_dim_address.createOrReplaceTempView('user')
                os.system('hdfs dfs -rm -f -R /'+app_args.pais+'/user_csv')
                # escribe en hadoop el resultado del join en la carpeta del pais
                df_user.coalesce(1).write.csv('hdfs://hadoop-namenode:9000/'+app_args.pais+'/user_csv/', mode='overwrite', sep=';')
        except:
                print traceback.format_exc()
def get_app_args():
        parser = argparse.ArgumentParser()
        parser.add_argument("-p", "--pais", help="primeras dos letras del pais: uy, cl, ar, br, pa, py, bo, co")
        return parser.parse_args()

if __name__ == '__main__':
        app_args = get_app_args()
        user_csv(app_args)
