/home/hduser/spark/bin/spark-submit --master spark://hadoop-namenode:7077 --driver-memory 3G --driver-cores 2 --executor-memory 3G --conf spark.cores.max=2 $TO_HDFS/redshift_partners_by_country_to_hdfs.py -p $1