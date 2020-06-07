### project address

* github

https://github.com/harryhare/udacity_data_engineer_capstone_project


因为总是感觉project 中有些东西没说清楚，有没法方便的联系到mentor，发现了一些可以帮助完成 project 方法：

* https://knowledge.udacity.com/ 查答案

* github 上搜关键字 , 比如 data engineering 这门课 就可以搜  `oad_songplays_fact_table`

### pyspark

* doc

http://spark.apache.org/docs/latest/api/python/search.html

* pyspark cheatsheet


```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import isnan, count, when, col, desc, udf, col, sort_array, asc, avg
from pyspark.sql.functions import sum as Fsum
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

import pandas as pd
df.toPandas()
pd.set_option('max_colwidth', 200)

df.show()
df.show(5)
df.take(5) // return array
df.limit(5) //return dateframe

df.count()


df.withColumnRenamed('c1', 'c2')
df.withColumn("c1",col("c2").cast(DecimalType(5,2))) 
df.withColumn("c1",col("c1").cast(IntegerType))) # 替换 列 c1 的数据类型  

df.filter(df.c1=="1234")
df.filter(col("c1")=="1234")

df.select("c1","c2")
df.select(col("c1"),col("c2"))
df.select(df.c1, df2,c2)
df.select(df.c1.alias("c2"))

df.groupBy(["c1","c2"]).count()
df.groupBy(["c1","c2"]).sum("xxx")
df.agg({'Artist':'count'}) 


df.orderBy(desc("count"))


df=df1.join(df2,df1.c1==df2.c2)


df.createOrReplaceTempView("table1") # createOrReplaceTempView 和 sql 配合使用
spark.sql("SELECT * FROM table1 LIMIT 2")
spark.sql('''
          SELECT * 
          FROM table1 
          LIMIT 2
          '''
          ).show()


import  pyspark.sql.functions as F
df = df.withColumn("date", F.to_timestamp("date"))


def get_longitude(x):
    i=x.find(",")
    return float(x[:i])
def get_latitude(x):
    i=x.find(",")
    return float(x[i+2:])

get_longitude_udf = udf(lambda x: get_longitude(x), DoubleType())
get_latitude_udf = udf(lambda x: get_latitude(x), DoubleType())

df = df.withColumn('longitude', get_longitude_udf('coordinates'))
df = df.withColumn('latitude', get_latitude_udf('coordinates'))


```

* 初始化
```python
spark = SparkSession \
    .builder \
    .appName("Data wrangling with Spark SQL") \
    .getOrCreate()
	
spark = SparkSession.builder \
	.config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
	.enableHiveSupport() \
	.getOrCreate()

spark = SparkSession.builder.getOrCreate()
```


* 读数据
```python
df = spark.read.json("input1.json")
df = spark.read.text("data/NASA_access_log_Jul95.gz")
df = spark.read.csv("s3a://udacity-dend/pagila/payment/payment.csv")
df = spark.read.csv("s3a://udacity-dend/pagila/payment/payment.csv",sep=";", inferSchema=True, header=True)

schema = StructType([
    StructField('id', StringType(), True), # 第三个参数表示允许为空
    StructField('name', StringType(), True),
])
df=spark.read.csv("data.csv",schema=schema,sep=";")
df=spark.read.option("header", True).csv("data.csv",schema=schema)

df=spark.read.schema(schema).parquet("../intermediate/sas_data")
```

* 写数据
```python

df.write.mode('overwrite').partitionBy("year","month").parquet("output_dir/")
df.write.mode('overwrite').parquet("output_dir/")

```


* 添加自增id
https://www.cnblogs.com/sitoi/p/11819610.html

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.createDataFrame(
    [
        {"name": "Alice", "age": 18},
        {"name": "Sitoi", "age": 22},
        {"name": "Shitao", "age": 22},
        {"name": "Tom", "age": 7},
        {"name": "De", "age": 17},
        {"name": "Apple", "age": 45}
    ]
)
df.show()

df = df.withColumn("id", monotonically_increasing_id())
df.show()

from pyspark.sql.functions import row_number

spec = Window.partitionBy().orderBy("age")
df = df.withColumn("id", row_number().over(spec))
df.show()

```


* window
```
# TODO: filter out 0 sum and max sum to get more exact answer

function = udf(lambda ishome : int(ishome == 'Home'), IntegerType())

user_window = Window \
    .partitionBy('userID') \
    .orderBy(desc('ts')) \
    .rangeBetween(Window.unboundedPreceding, 0)

cusum = df.filter((df.page == 'NextSong') | (df.page == 'Home')) \
    .select('userID', 'page', 'ts') \
    .withColumn('homevisit', function(col('page'))) \
    .withColumn('period', Fsum('homevisit').over(user_window))

cusum.filter((cusum.page == 'NextSong')) \
    .groupBy('userID', 'period') \
    .agg({'period':'count'}) \
    .agg({'count(period)':'avg'}).show()
```