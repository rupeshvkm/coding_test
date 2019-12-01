from pyspark import SparkContext
from pyspark.sql import Window,SparkSession
from pyspark.sql.functions import *
import pandas as pd

sc = SparkContext(master="local",appName="spark-test")
spark = SparkSession(sc)
################################################Question - 3#######################################################
email_list = ["abc@def.co.in","cvb2@strd.eu","AbD@sbcg3","cvf@&shd.com"]
cols = ["email","is_valid"]
def validateEmail(str):
    regex = '^\w+([\.-]?\w+)*@\w+([\.-]?\w+)*(\.\w{2,3})+$'
    mail = str.split('@')
    if len(mail)<2:
        return False
    name = mail[0].lower()
    domain = mail[1].lower()
    domain_val = domain.split('.')
    if not name :
        return False
    if len(domain_val)<2:
        return False
    valid_chars = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n',
                   'o','p','q','r','s','t','u','v','w','x','y','z','1','2','3','4','5','6','7','8','9','0','.','_','-']
    for i in name:
        if i not in valid_chars:
            return False
    for j in domain:
        if j not in valid_chars:
            return False

    print(str)
    return True
#pyspark implementation

emails = sc.parallelize(email_list)
email_rdd = emails.map(lambda r : (r,validateEmail(r)))
email_df = email_rdd.toDF(cols)

#pandas implementation
pd_df = pd.DataFrame(email_list,columns = ['email'])
pd_df["is_valid"] = pd_df["email"].map(validateEmail)



##############################################Question - 4####################################################

a_list = pd_df.columns.map(lambda l:'a_'+l)

# Pandas implementation
pd_dict = pd_df.to_dict()
for c in cols:
    new_col = 'a_'+c
    pd_dict[new_col] = pd_dict.pop(c)

dict_to_df = pd.DataFrame(pd_dict)

#pyspark implementation
pyspark_dict = email_df.rdd.collect()
row_list = []
for i in pyspark_dict:
    row_list.append((i.__getitem__("email"),i.__getitem__("is_valid")))
new_cols = []
for i in cols:
    new_cols.append('a_'+i)
sc.parallelize(row_list).toDF(new_cols)

###########################################Question - 5###################################################
#zipWithIndex
#costly operation: df -> rdd -> df
zipped_df = email_df.rdd.zipWithIndex().toDF()
zipped_df.withColumn("email",col("_1").getItem("email")).withColumn("is_valid",col("_1").getItem("is_valid")).withColumnRenamed("_2","index").show()

#row_number
#df need to be sorted based on column
email_df.withColumn("index",row_number().over(Window.orderBy(col("email"))))

#Cannot use monotonically increasing id because index will not be consecutive over distributed system