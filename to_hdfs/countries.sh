/home/hduser/spark/bin/spark-submit --master spark://hadoop-namenode:7077 --driver-memory 2G --driver-cores 2 --executor-memory 2G --conf spark.cores.max=2 $TO_HDFS/redshift_countries_to_hdfs.py