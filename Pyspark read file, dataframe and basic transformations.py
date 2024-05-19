# Databricks notebook source
spark

# COMMAND ----------

# MAGIC %md
# MAGIC To view the file in a directory

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/

# COMMAND ----------

# MAGIC %md
# MAGIC HOW TO READ A CSV FILE

# COMMAND ----------

example_df=spark.read.format('csv')\
            .option('header','True')\
            .option('inferschema','True')\
            .load('/FileStore/tables/2010_summary.csv')
example_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC PRINT SCHEMA
# MAGIC * Schema is made of column name and its data type. 

# COMMAND ----------

example_df=spark.read.format('csv')\
            .option('header','True')\
            .option('inferschema','True')\
            .load('/FileStore/tables/2010_summary.csv')
example_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC CREATE MANUAL SCHEMA
# MAGIC * Data Types we have in Struct Field https://spark.apache.org/docs/latest/sql-ref-datatypes.html
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType
myschema= StructType(
    [
        StructField('dest_country_name',StringType(),True),
        StructField('original_country_name',StringType(),True),
        StructField('count',IntegerType(),True),
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC PASSING CSV FILE FROM MANUAL SCHEMA

# COMMAND ----------

df_schema= spark.read.format('csv')\
                    .option('header','False')\
                    .option('inferschema','True')\
                    .option("SkipRows",'3')\
                    .option('mode','PERMISSIVE')\
                    .schema(myschema)\
                    .load('/FileStore/tables/2010_summary.csv')
df_schema.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC Handling corrupt records in csv file

# COMMAND ----------

'''from pyspark.sql.types import StructField, StructType, StringType, IntegerType
mys_chema=StructType(
    [
        StructField('id',IntegerType(),False),
        StructField('Name',StringType(),False),
        StructField('Age', IntegerType(),False),
        StructField('Salary', IntegerType(),False),
        StructField('Address',StringType(),True),
        StructField('nominee', StringType(),True),
        StructField('_courupt_record',StringType(),True)
    ]
)'''
read_df= spark.read.format('csv')\
        .option('header','True')\
        .option('inferschema','True')\
        .option('mode','Permissive')\
        .load('/FileStore/tables/data1-1.csv')
read_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Filter out corrupt records from a csv file

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType
mys_chema=StructType(
    [
        StructField('id',IntegerType(),False),
        StructField('Name',StringType(),False),
        StructField('Age', IntegerType(),False),
        StructField('Salary', IntegerType(),False),
        StructField('Address',StringType(),True),
        StructField('nominee', StringType(),True),
        StructField('_corrupt_record',StringType(),True)
    ]
)
read_df= spark.read.format('csv')\
        .option('header','True')\
        .option('inferschema','True')\
        .option('mode','Permissive')\
        .schema(mys_chema)\
        .load('/FileStore/tables/data1-1.csv')
read_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC write bad record in a file/path

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType
mys_chema=StructType(
    [
        StructField('id',IntegerType(),False),
        StructField('Name',StringType(),False),
        StructField('Age', IntegerType(),False),
        StructField('Salary', IntegerType(),False),
        StructField('Address',StringType(),True),
        StructField('nominee', StringType(),True),
        StructField('_corrupt_record',StringType(),True)
    ]
)
read_df= spark.read.format('csv')\
        .option('header','True')\
        .option('inferschema','True')\
        .schema(mys_chema)\
        .option('badRecordsPath','/FileStore/tables/badrecord3')\
        .load('/FileStore/tables/data1-1.csv')
read_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC check file for bad record

# COMMAND ----------

bad_record=spark.read.format('json').load('/FileStore/tables/badrecord3/20231104T035522/bad_records/part-00000-57368d2b-b485-43bf-afa8-8f1e3396658a')
bad_record.show(truncate=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #JSON
# MAGIC JSON is a text-based format that consists of key-value pairs and arrays.
# MAGIC Object: 
# MAGIC * An object in JSON is a collection of key-value pairs. It is enclosed in curly braces {}.
# MAGIC * Keys must be strings enclosed in double quotes, followed by a colon, and then a value.
# MAGIC * Keys are unique within an object.
# MAGIC * Values can be any valid JSON data type, including objects, arrays, strings, numbers, booleans, or null.  
# MAGIC
# MAGIC Here's an example combining these types within a JSON object:
# MAGIC
# MAGIC {  
# MAGIC   "person": {  
# MAGIC     "name": "Bob",  
# MAGIC     "age": 35,  
# MAGIC     "isStudent": false  
# MAGIC   },  
# MAGIC   "fruits": ["apple", "banana", "cherry"],  
# MAGIC   "location": {  
# MAGIC     "city": "Los Angeles",  
# MAGIC     "zipCode": null  
# MAGIC   }  
# MAGIC }  
# MAGIC In this example, the JSON object contains an object ("person"), an array ("fruits"), and another object ("location"). The values of the object can be strings, numbers, booleans, or null, and the array contains strings. These types can be nested within each other to represent complex data structures.  
# MAGIC **Types of JSON**  
# MAGIC * Line-Delimited JSON (JSONL):  
# MAGIC   Structure: In line-delimited JSON, each line of text represents a separate JSON object or value. The lines are typically separated by line breaks. This format allows you to handle multiple JSON objects separately.  
# MAGIC   {"name":"Alice","age":28,"city":"New York"}  
# MAGIC   {"name":"Bob","age":35,"city":"Los Angeles"}  
# MAGIC * Multi-Line JSON:  
# MAGIC   Structure: Multi-line JSON involves breaking the JSON data into multiple lines with proper indentation and line breaks for human readability. This format is used for easy manual editing and understanding of the data's structure.  
# MAGIC   [  
# MAGIC   {  
# MAGIC     "name": "Alice",  
# MAGIC     "age": 28,  
# MAGIC     "city": "New York",  
# MAGIC     "hobbies": ["reading", "hiking"],  
# MAGIC     "isActive": true  
# MAGIC   },  
# MAGIC   {  
# MAGIC     "name": "Bob",  
# MAGIC     "age": 35,  
# MAGIC     "city": "Los Angeles",  
# MAGIC     "hobbies": ["swimming", "cooking"],  
# MAGIC     "isActive": false  
# MAGIC   },  
# MAGIC   {  
# MAGIC     "name": "Catherine",  
# MAGIC     "age": 22,  
# MAGIC     "city": "Chicago",  
# MAGIC     "hobbies": ["painting", "yoga"],  
# MAGIC     "isActive": true  
# MAGIC   }  
# MAGIC ]  
# MAGIC   
# MAGIC
# MAGIC **Note: Line delimited JSON is faster that Multiline JSON. As in Line Delimited JSON it knows that one object will be in one row but in multiline line json it will first treat whole json as one object and then look where brackets are closed.**    
# MAGIC * **By default JSON is Line delimited JSON** 
# MAGIC
# MAGIC

# COMMAND ----------

#File paths for easy access
File uploaded to /FileStore/tables/line_delimited_json_json.txt
File uploaded to /FileStore/tables/corrupted_json1.json
File uploaded to /FileStore/tables/Multi_line_incorrect_json.txt
File uploaded to /FileStore/tables/single_file_json_with_extra_fields_json.txt
File uploaded to /FileStore/tables/Multi_line_correct1.json

# COMMAND ----------

# How to read a JSON file
#Line delimited Json
df_line_delimited= spark.read.format('json')\
            .option('inferschema','True')\
            .option('mode','Permissive')\
            .load('/FileStore/tables/line_delimited_json_json.txt')
df_line_delimited.show()


# COMMAND ----------

# Line delimited Json with extra fields
df_extra_fields= spark.read.format('json')\
            .option('inferschema','True')\
            .option('mode','Permissive')\
            .load('/FileStore/tables/single_file_json_with_extra_fields_json.txt')
df_extra_fields.show()

# COMMAND ----------

# multiline Json
#By default JSON is Line delimited JSON. So if you try to read multiline json, you have to specify it explicitly.
df_extra_fields= spark.read.format('json')\
            .option('inferschema','True')\
            .option('mode','Permissive')\
            .option('multiline', 'True')\
            .load('/FileStore/tables/Multi_line_correct1.json')
df_extra_fields.show()

# COMMAND ----------

# multiline Json
#By default JSON is Line delimited JSON. So if you try to read multiline json, you have to specify it explicitly.
df_mi= spark.read.format('json')\
            .option('inferschema','True')\
            .option('mode','Permissive')\
            .option('multiline', 'True')\
            .load('/FileStore/tables/Multi_line_incorrect_json.txt')
df_mi.show()

# COMMAND ----------

# corrupted Json
df_extra_fields= spark.read.format('json')\
            .option('inferschema','True')\
            .option('mode','Permissive')\
            .load('/FileStore/tables/corrupted_json1.json')
df_extra_fields.show(truncate=False)

# COMMAND ----------

# MAGIC %md 
# MAGIC #Parquet file format

# COMMAND ----------

df=spark.read.parquet("/FileStore/tables/part_r_00000_1a9822ba_b8fb_4d8e_844a_ea30d0801b9e_gz.parquet")
df.show()

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/')

# COMMAND ----------

# MAGIC %md
# MAGIC ## DATAFRAME WRITER API

# COMMAND ----------

A_dF=spark.read.format('csv')\
                .option('header','True')\
                .load('/FileStore/tables/partition.csv')
A_dF.show()

# COMMAND ----------

# Assume above is the table we want to write on the disk after transformaitons. will will write it as:
A_dF.write.format('csv')\
        .option('header','True')\
        .mode('overwrite')\
        .option('path','/FileStore/tables/csv_write1/')\
        .save()

# COMMAND ----------

#To check the name of the file
dbutils.fs.ls('/FileStore/tables/csv_write1/')

# COMMAND ----------

# accessing the file after writing it to disk
a=spark.read.format('csv')\
            .option('header','True')\
            .load('/FileStore/tables/csv_write1/part-00000-tid-1940807798308584405-7706d20c-a9b5-4492-826e-a55a86c96c0f-8-1-c000.csv')
a.show()

# COMMAND ----------

# if we want to do repartition: It will try to write the records in 3 different files of similar size. 
# Data in repartition will be shuffled in a way that size of all the files will be near equal. so records will be random in the files.
A_dF.repartition(3).write.format('csv')\
        .option('header','True')\
        .mode('overwrite')\
        .option('path','/FileStore/tables/csv_write2/')\
        .save()
#To check the name of the file
dbutils.fs.ls('/FileStore/tables/csv_write2/')

# COMMAND ----------

# accessing the file after writing it to disk
a=spark.read.format('csv')\
            .option('header','True')\
            .load('/FileStore/tables/csv_write2/part-00000-tid-7298306772831041254-4c3c0da9-5a4c-4935-9899-5c22deac7679-28-1-c000.csv')
a.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Partition by and bucketing
# MAGIC Both the concepts come into play when you try to write the data on the disk 

# COMMAND ----------

#Doing Partitions on Address column
A_dF.write.format('csv')\
            .option('header','True')\
            .mode('overwrite')\
            .option('Path','/FileStore/tables/partition_by_address/')\
            .partitionBy('address')\
            .save()

# COMMAND ----------

# To access the files after partition by
dbutils.fs.ls('/FileStore/tables/partition_by_address/')

# COMMAND ----------

# making 3 buckets out of the csv file.
# Save is not supported by bucketBy so we need to save it as table and need to give table name as below
A_dF.write.format('csv')\
        .option('header','True')\
        .option('path','/FileStore/tables/bucketing_by_3/')\
        .mode('overwrite')\
        .bucketBy(3,'id')\
        .saveAsTable('bucketing_by_3_tableName')

# COMMAND ----------

#access files after bucketBy
dbutils.fs.ls('/FileStore/tables/bucketing_by_3/')

# COMMAND ----------

#Accessing files after bucketBy
df1=spark.read.format('csv').option('header','True').load('/FileStore/tables/bucketing_by_3/part-00000-tid-2750834127748509013-17400b6a-7898-4bd4-8055-fd9aaea7ce5f-5-1_00000.c000.csv')
df2=spark.read.format('csv').option('header','True').load('/FileStore/tables/bucketing_by_3/part-00000-tid-2750834127748509013-17400b6a-7898-4bd4-8055-fd9aaea7ce5f-5-2_00001.c000.csv')
df3=spark.read.format('csv').option('header','True').load('/FileStore/tables/bucketing_by_3/part-00000-tid-2750834127748509013-17400b6a-7898-4bd4-8055-fd9aaea7ce5f-5-3_00002.c000.csv')
df1.show()
df2.show()
df3.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### How to create dataframe in spark 
# MAGIC * Schema is made up of column name and column data type.
# MAGIC * Dataframe is made up of columns and rows.

# COMMAND ----------

 my_data = [(1, 1),   
 (2, 1),   
 (3, 1),   
 (4, 2),   
 (5, 1),   
 (6, 2),   
 (7, 2)]

# COMMAND ----------

# to create the schema for above data frame.
# we are doing it like this since we are not reading the data.
my_schema=['id','num']

# COMMAND ----------

my_dataframe = spark.createDataFrame(data=my_data,schema=my_schema)
my_dataframe.show()

# COMMAND ----------

employee_df=spark.read.format('csv')\
                .option('header','True')\
                .load('/FileStore/tables/partition.csv')
employee_df.show()

# COMMAND ----------

# TO SELECT A PARTICULAR COLUMN
# WE HAVE MULTIPLE WAYS TO DO THAT
  # STRING METHOD
  # COLUMN METHOD 

# COMMAND ----------

#STRING METHOD
employee_df.select('id').show()

# COMMAND ----------

#column method
# to use column method we need to import column.
# using below imports we can use almost all the things needed to use sql in spark 
from pyspark.sql.functions import *
from pyspark.sql.types import *
employee_df.select(col('id')).show()

# COMMAND ----------

# use of string method is helpful we need to select multiple columns from the df as it is more convinient
# use of column method is helpful when we do transformations on columns. like if we try to add 5 in id column string method will give error but we can do that easily in column method as shown below.

# COMMAND ----------

employee_df.select('id+5').show()
#this error is resolved later on using expressions

# COMMAND ----------

# MAGIC %md
# MAGIC Aliasing

# COMMAND ----------

employee_df.select((col('id') + 5).alias('employee_id'),"name","salary").show()

# COMMAND ----------

# Selecting multiple columns
employee_df.select('id','name','age').show()
employee_df.select(col('id'),col('name'),col('age')).show()

# COMMAND ----------

# all the ways to select columns 
employee_df.select("id",col("name"),employee_df["salary"],employee_df.address).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Expression
# MAGIC * we can write sql like statement inside a expr()

# COMMAND ----------

employee_df.select(expr('id + 5 as new_id')).show()
employee_df.select(expr('concat(name,salary) as name_salary')).show()
employee_df.select('*').show()

# COMMAND ----------

# MAGIC %md
# MAGIC Spark sql 
# MAGIC * To work with spark sql we need to convert the dataframe in temporary view 

# COMMAND ----------

# to create a temporary view
employee_df.createOrReplaceTempView("employee_table")

# COMMAND ----------

#general practice is to use """ """ for sql.
spark.sql("""

select * from employee_table

""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Filter in spark
# MAGIC * filter and where is same. there is no difference

# COMMAND ----------

# No need for select statement if you want to retreive all the columns. By default it will give you all the columns.
employee_df.filter(col("salary")>=150000).show()
employee_df.where(col("salary")>=150000).show()

# COMMAND ----------

# multiple filter conditions
employee_df.filter((col("salary")>=150000) & (col('age')>=18)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Literal
# MAGIC * we have so many case where we want to pass same value to all the records in a particular column. This is where literal come in the picture.
# MAGIC * the term "literal" is used to represent a fixed value in a query or expression.

# COMMAND ----------

employee_df.select('*',lit('Sharma').alias('Last_name')).show()

# COMMAND ----------

# MAGIC %md 
# MAGIC With Column
# MAGIC * If you want or manipulate a existing column or create a new column with column will help you in that.
# MAGIC * if already a column is present, then it will override that
# MAGIC * first we need to give column name and then the expression 

# COMMAND ----------

new_employee_df=employee_df.withColumn('sur_name',lit('Kumar'))
new_employee_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Rename column

# COMMAND ----------

new_employee_df.withColumnRenamed('sur_name','Last_name').show()

# COMMAND ----------

# MAGIC %md
# MAGIC Casting Data Type

# COMMAND ----------

employee_df.printSchema()

# COMMAND ----------

employee_df.withColumn('id',col('id').cast('integer')).printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC Remove columns

# COMMAND ----------

new_employee_df.drop('sur_name').show()

# COMMAND ----------



# COMMAND ----------

