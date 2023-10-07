from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, count, when, col

# spark = SparkSession.builder.appName('CountTest').getOrCreate()
# data = spark.sparkContext.parallelize([1,2,3,4,5])
# cleaned_data = data\
#     .filter(lambda x: x != 1)\
#     .map(lambda x: x + 1)\
#     .reduce()



# 创建 SparkSession
spark = SparkSession.builder.appName("reduceExample").getOrCreate()

# 创建一个包含整数的RDD
data = [1,2,3,4,5]

sc = spark.sparkContext.parallelize(data)
# 使用 reduce 函数将 RDD 中的元素相加
result = sc.count()

# 打印结果
print("Sum of elements:", result)

