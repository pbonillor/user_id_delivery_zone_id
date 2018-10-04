/home/hduser/spark/bin/spark-submit --master spark://hadoop-namenode:7077 --driver-memory 7G --driver-cores 4 --executor-memory 7G --conf spark.cores.max=4 /home/hduser/spark/apps/user.py -p $1
