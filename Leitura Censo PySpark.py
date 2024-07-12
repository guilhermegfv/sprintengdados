# Databricks notebook source
import json
import requests
from pyspark.sql.functions import explode, col, split, trim, expr

# COMMAND ----------

url = 'https://apisidra.ibge.gov.br/values/t/4714/v/allxp/p/2022/n6/all?formato=json' 
response = requests.get(url)


if response.status_code == 200:
    data = response.json()
else:
    print(f"Error: {response.status_code}")
    print(response.text)
    data = None

if data:
    df = spark.read.json(spark.sparkContext.parallelize([data]))
    #df.show()

# COMMAND ----------

new_columns = df.first()
df = df.toDF(*new_columns)
df = df.filter(df[new_columns[0]] != new_columns[0])

# COMMAND ----------

split_col = split(df["Município"], ' - ')
df = df.withColumn("Cidade", trim(split_col.getItem(0)))
df = df.withColumn("Estado", trim(split_col.getItem(1)))

# COMMAND ----------

for col_name in df.columns:
    new_col_name = col_name.replace('(', '').replace(')', '').replace(' ', '')
    df = df.withColumnRenamed(col_name, new_col_name)

# COMMAND ----------

df.repartition(20)\
              .write.format("delta")\
              .mode("overwrite")\
              .partitionBy('Estado')\
              .option("overwriteSchema", "true")\
              .save("/dbfs/censo")

# COMMAND ----------

df.display()

# COMMAND ----------

https://apisidra.ibge.gov.br/values/t/4714/n6/all/v/all/p/all/d/v614%202,v6318%203
