import pyspark
from pyspark.sql import SparkSession 
from pyspark.sql.functions import col, explode, posexplode, min

import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Creating a SparkSession
spark = SparkSession.builder \
        .master("local[1]").appName("SparkByExamples.com") \
        .getOrCreate()

# Creating a RDD from a Text File
rdd2 = spark.sparkContext.textFile(r"C:\Users\jakem\Downloads\testing_test_file_for_spark.txt")

# Can perform "Transformations" or "Actions" on a RDD

# Creating a DataFrame
data = [("Jose", "Television", 2000),
        ("Jill", "Cycling", 6000),
        ("Bart", "Drawing", 500),
        ("Jane", "Drawing", 200)]

columns = ["Name", "Hobby", "Expenditure"]
df = spark.createDataFrame(data=data, schema=columns)

# df.printSchema()
# df.show()

# Renaming a column of a DataFrame with withColumneRenamed() function
df2 = df.withColumnRenamed("Expenditure", "Hobby Cost")

# Updating value of a column with withColumn() function
df2.withColumn("Hobby Cost", col("Hobby Cost") * 10).show()

# Filtering a DataFrame with the filter() function (where() performs the same)
df2.filter(df2.Name == 'Jose').show()
df2.filter((df2.Name == 'Jose') & (df2.Hobby == 'Television'))
acceptable_hobbies = ['Television', 'Cycling']
df2.filter(df2.Hobby.isin(acceptable_hobbies)).show()
df2.filter(df.Hobby.like("%T%")).show() # Using a regular expression to filter

# Sorting a DataFrame with the sort() function
print("-----Sorting Section-----")
df2.sort('Hobby Cost', ascending=True).show()
df2.sort('Hobby Cost', "Name", ascending=[True, True]).show()

# Creating a new DataFrame with map elements
print("-----Map Section-----")
arrayData = [
        ('James', ['Java','Scala'], {'hair':'black','eye':'brown'}),
        ('Michael', ['Spark','Java',None], {'hair':'brown','eye':None}),
        ('Robert', ['CSharp',''], {'hair':'red','eye':''}),
        ('Washington', None, None),
        ('Jefferson', ['1','2'] ,{})]

df_array = spark.createDataFrame(data=arrayData, schema = ['name','knownLanguages','properties'])

# Using explode() to get all known languages in an array
df_array.select(df_array.name, explode(df_array.knownLanguages)).show()

# Using explode() to get all known properties in an map (creates two new columms; one for key, one for value)
df_array.select(df_array.name, explode(df_array.properties)).show()

# Use posexplode() to also get an index value for each element in the array or map
df_array.select(df_array.name, posexplode(df_array.properties)).show()

# By default, if a value is null, explode will not include it in the returned rows. Use explode_outer() to return nulls

# Reading and writing data
print("-----Reading and writing data section-----")
#parquet_file = df2.write.parquet("written_files/df2.parquet")

# Groupby
print("-----Groupby section-----")
df_group = df2.groupby("Hobby").mean("Hobby Cost").show()

simpleData = [("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  ]

# Create DataFrame
schema = ["employee_name","department","state","salary","age","bonus"]
df_aggregate = spark.createDataFrame(data=simpleData, schema = schema)

# Remember to import Spark's min aggregation function
df_aggregate_2 = df_aggregate.groupby("department", "state").agg(min("age").alias("youngest"), min("bonus").alias("smallest")) \
.where(col("smallest") > 20000).show()

# Using a SQL technique
df_aggregate.createOrReplaceTempView("Employees")
sql_query = "SELECT department, state, min(age) AS youngest, min(bonus) AS smallest from Employees GROUP BY department, state HAVING smallest > 20000"
df_aggregate_sql = spark.sql(sql_query)
df_aggregate_sql.show()