from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("sparkJoin").getOrCreate()

countries = spark.read.option("header","true").csv("D:\juniper-coding-test\data/countries.csv")

country_city = spark.read.option("header","true").csv("D:\juniper-coding-test\data/CountryCityMap.csv")

countries.join(country_city,['country'],'left').show()