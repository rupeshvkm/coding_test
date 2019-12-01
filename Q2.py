from pyspark import SparkContext
from pyspark.sql import SparkSession


sc = SparkContext(master="local",appName="spark-test").getOrCreate()

#q2
spark = SparkSession(sc)
spark.sql("set hive.merge.mapredfiles = false")
spark.sql("set hive.merge.smallfiles.avgsize = 16000000")
spark.sql("set hive.execution.engine = mr")