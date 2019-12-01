import pandas as pd
import glob
from pyspark.sql import SparkSession

li = []

spark = SparkSession.builder.master("local").appName("pandasToSpark").getOrCreate()

for filename in glob.glob('D:\juniper-coding-test\data/*.csv'):
    df = pd.read_csv(filename, index_col=None, header=0,sep=None)
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    li.append(df)

frame = pd.concat(li, axis=0, ignore_index=True)


spark.createDataFrame(frame)

