from pyspark import SparkConf,SparkContext


hdfs_name_node_ip = 'x.x.x.x'
hdfs_name_node_port = '1234'
hdfs_user_name = 'test'
file_path = '/user/logger/python/logs'
file_name = 'test.log'

spark_eventLog_enabled = "true"
spark_eventLog_dir = "hdfs://"+hdfs_name_node_ip+':'+hdfs_name_node_port+"/"+hdfs_user_name+"/"
spark_history_fs_logDirectory = "hdfs://"+hdfs_name_node_ip+':'+hdfs_name_node_port+"/"+hdfs_user_name+"/"+file_path+"/"
spark_logConf = True



#q1
conf = (SparkConf().setAppName("test_code"))\
    .set("spark.eventLog.enabled",spark_eventLog_enabled)\
    .set("spark.eventLog.dir",spark_eventLog_dir)\
    .set("spark.history.fs.logDirectory",spark_history_fs_logDirectory)


sc = SparkContext(master="local",appName="Spark_Demo",conf=conf).getOrCreate()