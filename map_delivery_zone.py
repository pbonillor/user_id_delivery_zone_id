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
def map_delivery_zone(app_args):
        try:
                # asigna a la variable con el nombre de la aplicacion que se ejecutara en spark, le pasa como parametro el pais
                conf = SparkConf().setAppName("map_delivery_zone_"+app_args.pais)
                # se crea la variable sc que define el contexto de ejecucion con el parametro de configuracion
                sc = SparkContext(conf=conf)
                # se crea la variable sqlContext para realizar la consulta
                sqlContext = SQLContext(sc)
                df_delivery_zone = sqlContext.read.parquet('hdfs://hadoop-namenode:9000/'+app_args.pais+'/delivery_zone/*.parquet')
                df_delivery_zone.createOrReplaceTempView('delivery_zone')
                consulta = 'select delivery_zone.avg_food, delivery_zone.avg_rating, delivery_zone.avg_service, delivery_zone.avg_speed, delivery_zone.business_name, delivery_zone.business_type, delivery_zone.delivery_zone_min_delivery_amount, delivery_zone.delivery_zone_shipping_amount, delivery_zone.delivery_zone_shipping_amount_is_percentage, delivery_zone.main_cousine, delivery_zone.main_cousine_category_id, delivery_zone.restaurant_min_delivery_amount as partner_min_delivery_amount, delivery_zone.restaurant_shipping_amount as partner_shipping_amount, delivery_zone.restaurant_shipping_amount_is_percentage as partner_shipping_amount_is_percentage, delivery_zone.delivery_zone_id, delivery_zone.restaurant_name as partner_name, delivery_zone.delivery_zone_partner_id as partner_id, delivery_zone.name as delivery_zone_name, delivery_zone.surface * 100000 as surface, numbers.n AS sequence, SUBSTRING_INDEX(SUBSTRING_INDEX(delivery_zone.polygon, '+'\''+';'+'\''+', numbers.n), '+'\''+';'+'\''+', -1) AS point, REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(delivery_zone.polygon, '+'\''+';'+'\''+', numbers.n), '+'\''+';''\''+', -1),'+'\''+','+'\''+',1),'+'\''+'('+'\''+','+'\''+'\''+') latitude, REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(delivery_zone.polygon, '+'\''+';'+'\''+', numbers.n), '+'\''+';'+'\''+', -1),'+'\''+','+'\''+',-1),'+'\''+')'+'\''+','+'\''+'\''+') longitude from (SELECT ROW_NUMBER() OVER(order by 1) as n FROM (select 0 union all select 1 union all select 3 union all select 4 union all select 5 union all select 6 union all select 6 union all select 7 union all select 8 union all select 9) t,(select 0 union all select 1 union all select 3 union all select 4 union all select 5 union all select 6 union all select 6 union all select 7 union all select 8 union all select 9) t2, (select 0 union all select 1 union all select 3 union all select 4 union all select 5 union all select 6 union all select 6 union all select 7 union all select 8 union all select 9) t3,(select 0 union all select 1 union all select 3 union all select 4 union all select 5 union all select 6 union all select 6 union all select 7 union all select 8 union all select 9) t4,(SELECT 0) numbers) numbers INNER JOIN delivery_zone on CHAR_LENGTH(delivery_zone.polygon)-CHAR_LENGTH(REPLACE(delivery_zone.polygon, '+'\''+','+'\''+', '+'\''+'\''+'))>=numbers.n-1 WHERE is_deleted = 0'
                #print consulta
                df_map_delivery_zone = sqlContext.sql(consulta)
                os.system('hdfs dfs -rm -f -R /'+app_args.pais+'/map_delivery_zone')
                # escribe en hadoop el resultado del join en la carpeta del pais
                df_map_delivery_zone.write.parquet('hdfs://hadoop-namenode:9000/'+app_args.pais+'/map_delivery_zone/', mode='overwrite', compression='snappy')
        except:
                print traceback.format_exc()
def get_app_args():
        parser = argparse.ArgumentParser()
        parser.add_argument("-p", "--pais", help="primeras dos letras del pais: uy, cl, ar, br, pa, py, bo, co")
        return parser.parse_args()

if __name__ == '__main__':
        app_args = get_app_args()
        map_delivery_zone(app_args)
