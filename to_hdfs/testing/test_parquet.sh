/home/hduser/spark/bin/spark-submit --master spark://hadoop-namenode:7077 --driver-memory 7G --driver-cores 4 --executor-memory 7G --conf spark.cores.max=4 $TO_HDFS/test_parquet.py -p $1
