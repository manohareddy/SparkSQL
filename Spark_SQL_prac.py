from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *

spark = SparkSession \
        .builder \
        .appName("Practice") \
        .getOrCreate()

sc = spark.sparkContext

lines = sc.textFile('/data2/h516/practice/2015-12-12.csv')\
          .map(lambda x:x.split(','))

b = lines.map(lambda x:Row(date=x[0][1:-1], time=x[1][1:-1], size=x[2], rVersion=x[3][1:-1],arch=x[4][1:-1], os=x[5][1:-1],package=x[6][1:-1], version=x[7][1:-1],country=x[8][1:3],ip=x[9]))
df = spark.createDataFrame(b)
df.createOrReplaceTempView('db')

print("A1-")
spark.sql('select count(*) from db').show()

print('A4.2-')
spark.sql('select package,count(package) as mp from db where not package = "package" group by package order by mp desc limit 1').show()

print('A4.3-')
spark.sql('select country,count(country) as ct from db where not country = "co" group by country order by ct desc limit 1').show()

print('A5-')
spark.sql('select package,count(package) as ct from db where country ="DE" and not os="r_os" group by package order by ct desc limit 1').show()

print ('A6-')
spark.sql('select SUM(size) from db where country ="CA" and not country="co"').show()

print('A7-')
spark.sql('select version, count(version) as ct from db where time like "12%" and not os="r_os" group by version order by ct desc limit 1').show()

print('A8-')
spark.sql('select count(distinct os) from db where not os="r_os"').show()

print('A9-')
spark.sql('select country, os,  count(os) as ct from db where not os ="" group by country, os order by ct desc').show()

print('A10-')
spark.sql('select distinct ip, country from db').show()