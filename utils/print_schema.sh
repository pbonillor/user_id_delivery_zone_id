/home/hduser/spark/bin/spark-submit --master spark://hadoop-namenode:7077 --driver-memory 1G --driver-cores 1 --executor-memory 1G --conf spark.cores.max=1 $UTILS/print_schema.py -l $1
