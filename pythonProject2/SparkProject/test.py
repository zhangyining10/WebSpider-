from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, count, when, col
from pyspark.sql.types import *
import time

spark = SparkSession.builder.appName('NetEaseClouder').getOrCreate()

df = spark.read.csv("file_path.csv", header=True, inferSchema=True)
rdd = df.rdd

#读取csv文件
start = time.time()
df = spark.read.csv("D:\网易云大数据\大数据\impression_data.csv", inferSchema=True, header=True)
result = df.agg(mean('impressTime')).collect()[0][0]
result2 = df.agg(mean('mlogViewTime')).collect()[0][0]
finish = time.time()
print(finish-start)

null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
print(null_counts.show())

df.select("mlogId").count()

for c in df.columns:
    print(c, df.filter(col(c) == "NA").count())

import dask.dataframe as dd
start = time.time()
df2 = dd.read_csv("D:\网易云大数据\大数据\impression_data.csv")
print(df2.columns)
result = df2.impressTime.mean()
result2 = df2.mlogViewTime.mean()

print("Starting the computations...")
result = result.compute()
result2 = result2.compute()

finish = time.time()
print(result)
print(finish-start)


data = [1,2,3,4,5]

sc = spark.sparkContext.parallelize(data)
# 使用 reduce 函数将 RDD 中的元素相加
result = sc.count()

#定义schema信息
schema_string = "USER_ID NAME JOB INC OWN ACCEPT"
fields = [StructField(field, StringType(), True) for field in schema_string.split(" ")]

schema = StructType(fields)

#执行rdd操作
user_info = spark.sparkContext.textFile("hdfs://localhost: 8088/env/netcloud/env")\
    .map(lambda x: x.split(","))\
    .filter(lambda x: len(x) != 10)\
    .map(lambda x: Row(x[0], x[1].strip(), x[2].strip(), x[3].strip()))\
    .filter(lambda x: x[0] is not None and x[1] is not None and x[0] != "Anousous")

user_df = spark.createDataFrame(user_info, schema)

#定义mysql参数
mysql_url = "jdbc:mysql://localhost:3306/your_database"
mysql_properties = {
    "user": "your_username",
    "password": "your_password",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# 读取 JSON 数据
json_data = spark.read.json("hdfs://localhost:9000/path/to/your/data.json")

# 将 DataFrame 写入 MySQL
user_df.write.jdbc(mysql_url, "user_info", mode="overwrite", properties=mysql_properties)
