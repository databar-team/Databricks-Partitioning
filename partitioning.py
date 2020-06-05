#IMPORTANT
#EVRY "ENTER(new row) IS NEW CELL IN NOTEBOOK"

from pyspark.sql.functions import year, month, dayofmonth
from datetime import date, timedelta
from pyspark.sql.types import IntegerType, DateType, StringType, StructType, StructField

start_date = date(2019, 1, 1)
data = []
for i in range(0, 5):
    data.append({"Country": "CN", "Date": start_date +
                 timedelta(days=i), "Amount": 10+i})
    data.append({"Country": "AU", "Date": start_date +
                 timedelta(days=i), "Amount": 10+i})

schema = StructType([StructField('Country', StringType(), nullable=False),
                     StructField('Date', DateType(), nullable=False),
                     StructField('Amount', IntegerType(), nullable=False)])
df = spark.createDataFrame(data, schema=schema)
df.show()
print(df.rdd.getNumPartitions())

df.write.csv("/FileStore/test")

print(df.rdd.getNumPartitions())

df = df.coalesce(4)
print(df.rdd.getNumPartitions())
df.write.mode("overwrite").csv("/FileStore/test", header=True)

df = df.withColumn("Year", year("Date")).withColumn(
"Month", month("Date")).withColumn("Day", dayofmonth("Date"))

df = df.repartition("Year", "Month", "Day", "Country")
print(df.rdd.getNumPartitions())
df.write.mode("overwrite").csv("/FileStore/test", header=True)

df.write.partitionBy("Year", "Month", "Day", "Country").mode(
"overwrite").csv("/FileStore/test", header=True)

procitan_file = spark.read.csv("/FileStore/test", header = True)

procitan_file.show()

procitan_file.select("Date", "Day").filter(procitan_file["Day"]==3).show()

procitan_file.write.csv("FileStore/test/test2",header = True)