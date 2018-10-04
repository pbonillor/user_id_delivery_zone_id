# se importan las librerias argparse y ConfigParser para recibir parametros en las funciones
import argparse, ConfigParser
# se importan las librerias os y sys para enviar comandos al sistema operativo.
import os, sys
# se importa la libreria traceback para manejo de errores y excepciones
import traceback
# se importa la libreria time para realiza un sleep mientras que spark finaliza
from time import time
# se importa la libreria shapely a fin de realizar la conversion del punto y del poligono
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
# Se importa la libreria pyspark las funciones para declarar el contexto (SparkContext) y definir la configuracion (SparkConf)
from pyspark import SparkContext, SparkConf
# Se importa la libreria pyspark.sql la funcion para definir el contexto de una consulta (SQLContext)
from pyspark.sql import SQLContext
# Se importa la libreria pyspark.sql.types todas las funciones de los tipos de datos en las consultas
from pyspark.sql.types import *
# Se importa la libreria pyspark.sql la funcion Row
from pyspark.sql import Row
# Se importa la libreria tree de los indices espaciales
from rtree import index
# Se importa la libreria Process para el multiplocresamiento
from multiprocessing import Process, Queue

def invert_polygons(q):
	try:
		for polygonInMap in mapPolygons:
                        try:
                                polygonXY = eval(polygonInMap)
                                pointYX=()
                                polygonYX = []
                                for pointXY in polygonXY:
                                        x=pointXY[0]
                                        y=pointXY[1]
                                        pointYX=(y,x)
                                        polygonYX.append(pointYX)
                                list_polygons.append(polygonYX)
                        except:
                                print "error polygonInMap"
		q.put(list_polygons)
	except:
		print "error invert_polygons"

def obtain_polygons(q):
	try:
		from shapely.geometry import Polygon
                polygons = []
                for poly in list_polygons:
                        try:
                                pq = Polygon(poly)
                                polygons.append(pq)
                        except:
                                print "error list_polygons q"
		q.put(polygons)
	except:
		print "print error obtain_polygons"

def obtain_points(q):
	try:
		from shapely.geometry import Point
                points = []
                for poin in list_points:
                        try:
                                pp = Point(poin)
                                points.append(pp)
                        except:
                                print "error list_points q"
		q.put(points)
	except:
		print "error obtain_points"

def index_bounds_polygons():
        try:
                count = -1
                for q in polygons:
                    count +=1
                    idx.insert(count, q.bounds)
                return idx
        except:
                print "error index_bouns_polygons"

def search_point_polygons(q):
	try:
		quantity_iteration = (len(list_points) / 4000)
                print 'quantity interation: '+str(quantity_iteration)		
		for k in range(quantity_iteration):
			print 'k: '+str(k)
			address_delivery_zone = []
			x=4000*k
			y=(x+4000)-1
			for i in range(x,y): 
        	        	tupla = ()
                	     	for j in range(len(polygons)):
                        		if points[i].within(polygons[j]):
						tupla = (str(list_partner_id[j]),str(list_delivery_zone_id[j]),str(list_points[i][1]),str(list_points[i][0]))
                                        	address_delivery_zone.append(tupla)
 			rdd = sc.parallelize(address_delivery_zone)
	                rdd_address_delivery_zone = rdd.map(lambda x: Row(partner_id=x[0],delivery_zone_id=x[1],latitude=x[2],longitude=x[3]))
	                df_address_delivery_zone = sqlContext.createDataFrame(rdd_address_delivery_zone)
	                df_address_delivery_zone.write.parquet('hdfs://hadoop-namenode:9000/'+pais+'/address_delivery_zone_all/'+str(k)+'/', mode='overwrite', compression='snappy')
		q.put(address_delivery_zone)
	
	except Exception as e:
		print e

def delivery_zone_by_address_by_country_all(app_args):
	global mapPolygons
        global polygons
	global list_polygons
        global points
        global list_points
        global idx
        global list_partner_id
        global list_delivery_zone_id
        global address_delivery_zone
	global sc
	global sqlContext
	global pais

	try:
		pais = app_args.pais
	        mapPolygons = []
	        polygons = []
		list_polygons = []
		idx = index.Index()
	        points = []
	        list_points = []
	        list_partner_id = []
	        list_delivery_zone_id = []
	        address_delivery_zone = []

		# elimina el contenido de la carpeta de address_delivery_zone del pais
		os.system('hdfs dfs -rm -f -R /'+app_args.pais+'/address_delivery_zone_all/*')
		os.system('hdfs dfs -rm -f -R /'+app_args.pais+'/Points/*')

		# asigna a la variable con el nombre de la aplicacion que se ejecutara en spark, le pasa como parametro el pais
		conf = SparkConf().setAppName("delivery_zone_by_address_by_all_"+app_args.pais+"_to_hdfs")

		# se crea la variable sc que define el contexto de ejecucion con el parametro de configuracion
		sc = SparkContext(conf=conf)

		# se crea la variable sqlContext para realizar la consulta
		sqlContext = SQLContext(sc)

		df_dim_address = sqlContext.read.parquet('hdfs://hadoop-namenode:9000/'+app_args.pais+'/dim_address/*.parquet')
		df_dim_address.createOrReplaceTempView('dim_address')

                list_points = [] 

		points = sqlContext.sql("select DISTINCT(CONCAT('[',longitude,' , ',latitude,']')) as point from dim_address ORDER BY 1")
		mapPoints = points.rdd.map(lambda p: p.point).collect()
		
		for pointInMap in mapPoints:
			list_points.append(eval(pointInMap))

		pointsSchema = StructType([StructField("latitude", DoubleType()),StructField("longitude", DoubleType())])
	        df_points = sqlContext.createDataFrame(list_points,schema=pointsSchema)
                df_points.write.parquet('hdfs://hadoop-namenode:9000/'+app_args.pais+'/Points/', mode='overwrite', compression='snappy')

		df_delivery_zone = sqlContext.read.parquet('hdfs://hadoop-namenode:9000/'+app_args.pais+'/delivery_zone/*.parquet')
		df_delivery_zone.createOrReplaceTempView('delivery_zone')

		list_partner_id = []
                partner_ids = sqlContext.sql("select delivery_zone_partner_id as partner_id from delivery_zone WHERE polygon NOT LIKE "+"'%NaN%' ORDER BY delivery_zone_id DESC")
                mapPartnerIds = partner_ids.rdd.map(lambda p: p.partner_id).collect()

                for partnerIdInMap in mapPartnerIds:
               		list_partner_id.append(partnerIdInMap)

                list_delivery_zone_id = []
                delivery_zone_ids = sqlContext.sql("select delivery_zone_id as delivery_zone_id from delivery_zone WHERE polygon NOT LIKE "+"'%NaN%' ORDER BY delivery_zone_id DESC")
                mapDeliveryZoneIds = delivery_zone_ids.rdd.map(lambda p: p.delivery_zone_id).collect()

                for deliveryZoneIdInMap in mapDeliveryZoneIds:
                        list_delivery_zone_id.append(deliveryZoneIdInMap)

                polygons = sqlContext.sql("select CONCAT('(',REPLACE(REPLACE(delivery_zone.polygon,';',','),' ',''),')') as polygon from delivery_zone WHERE polygon IS NOT NULL AND polygon NOT LIKE "+"'%NaN%' ORDER BY delivery_zone_id DESC")
		mapPolygons = polygons.rdd.map(lambda p: p.polygon).collect()

		#print "mapPolygons: "+str(len(mapPolygons))

		q_invert_polygons = Queue()
		p_invert_polygons = Process(target=invert_polygons, args=(q_invert_polygons,))
		p_invert_polygons.start()		
		list_polygons = q_invert_polygons.get()
		p_invert_polygons.join()

		#print "list_polygons: "+str(len(list_polygons))

		q_obtain_polygons = Queue()
                p_obtain_polygons = Process(target=obtain_polygons, args=(q_obtain_polygons,))
                p_obtain_polygons.start()
                polygons = q_obtain_polygons.get()
                p_obtain_polygons.join()
	
		#print "polygons: "+str(len(polygons))

		q_obtain_points = Queue()
                p_obtain_points = Process(target=obtain_points, args=(q_obtain_points,))
                p_obtain_points.start()
                points = q_obtain_points.get()
                p_obtain_points.join()

		#print "points: "+str(len(points))

		indx = index_bounds_polygons()
		
		q_search_point_polygons = Queue()
                p_search_point_polygons = Process(target=search_point_polygons, args=(q_search_point_polygons,))
                p_search_point_polygons.start()
                address_delivery_zone = q_search_point_polygons.get()
                p_search_point_polygons.join()
	except:
		print traceback.format_exc()

def get_app_args():
	parser = argparse.ArgumentParser()
	parser.add_argument("-p", "--pais", help="primeras dos letras del pais: uy, cl, ar, br, pa, py, bo, co")
	return parser.parse_args()

if __name__ == '__main__':
	app_args = get_app_args()
	delivery_zone_by_address_by_country_all(app_args)
