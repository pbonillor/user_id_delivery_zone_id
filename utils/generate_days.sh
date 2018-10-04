/home/hduser/spark/bin/spark-submit --master spark://hadoop-namenode:7077 --driver-memory 1G --driver-cores 1 --executor-memory 1G --conf spark.cores.max=1 $UTILS/generate_days.py -fd $1 -td $2
