# Databricks notebook source
# MAGIC %md
# MAGIC UNION AND UNION ALL 
# MAGIC
# MAGIC In context of databricks, union and union all is same but in context of Spark sql or in general there are some key differences between them.
# MAGIC
# MAGIC UNION:
# MAGIC
# MAGIC * The UNION operator is used to combine the result sets of two or more SELECT statements and remove duplicate rows from the result.
# MAGIC * Each SELECT statement within the UNION must have the same number of columns in the result sets with similar data types and in the same order.
# MAGIC * Duplicate rows are eliminated, and only distinct rows are included in the final result.
# MAGIC
# MAGIC UNION ALL:
# MAGIC
# MAGIC * The UNION ALL operator is similar to UNION, but it includes all rows from the result sets, including duplicates.
# MAGIC * Unlike UNION, UNION ALL does not perform the extra step of removing duplicate rows, so it is generally faster than UNION.
# MAGIC * The columns in the SELECT statements must still have the same number of columns and compatible data types.

# COMMAND ----------

# MAGIC %md
# MAGIC DATABRICKS

# COMMAND ----------

#dataframe 1
data=[(10 ,'Anil',50000, 18),
(11 ,'Vikas',75000,  16),
(12 ,'Nisha',40000,  18),
(13 ,'Nidhi',60000,  17),
(14 ,'Priya',80000,  18),
(15 ,'Mohit',45000,  18),
(16 ,'Rajesh',90000, 10),
(17 ,'Raman',55000, 16),
(18 ,'Sam',65000,   17)]
schema=["id","name","sal","age"]
manager_df=spark.createDataFrame(data=data,schema=schema)
manager_df.show()
# Dataframe 2
data2=[(19 ,'Sohan',50000, 18),
(20 ,'Sima',75000,  17)]
manager_df2=spark.createDataFrame(data=data2,schema=schema)
manager_df2.show()

# COMMAND ----------

# To count records in multiple dataframe
a=manager_df.count()
b=manager_df2.count()
print(a,b,sep='\n')

# COMMAND ----------

#UNION
manager_df3=manager_df.union(manager_df2)
manager_df3.show()
manager_df3.count()

# COMMAND ----------

#union all 
manager_df3.unionAll(manager_df2).show()
manager_df3.unionAll(manager_df2).count()

# COMMAND ----------

#Union with duplicate records
manager_df3.union(manager_df2).show()
manager_df3.union(manager_df2).count()

# COMMAND ----------

# MAGIC %md
# MAGIC In context of spark sql 

# COMMAND ----------

manager_df3.createOrReplaceTempView('DF3')
manager_df2.createOrReplaceTempView('DF2')
spark.sql("""
          select * from DF3
          union
          select * from DF2
          """).count()

# COMMAND ----------

spark.sql("""
          select * from DF3
          union all
          select * from DF2
          """).show()

# COMMAND ----------

spark.sql("""
          select * from DF3
          union
          select * from DF2
          """).count()

# COMMAND ----------

spark.sql("""
          select * from DF3
          union all
          select * from DF2
          """).count()

# COMMAND ----------

# MAGIC %md 
# MAGIC What if we union values of wrong data type in a dataframe? 

# COMMAND ----------

wrong_column_data=[(19 ,50000, 18,'Sohan'),
(20 ,75000,  17,'Sima')]
wrong_column_data_schema=['id','sal','age','name']
wrong_data_df=spark.createDataFrame(data=wrong_column_data,schema=wrong_column_data_schema)
wrong_data_df.show()

# COMMAND ----------

manager_df.union(wrong_data_df).show()

# COMMAND ----------

# MAGIC %md
# MAGIC It will add the data as it is without validating the datatype
# MAGIC Alternate for this will is, unionByName. 
# MAGIC * But we can not use this every time coz in prod we will be working with more than 50 columns and matching all the columns by name will be very difficult.
# MAGIC * also it will not run if the column name is different. eg. name and emp name.

# COMMAND ----------

wrong_column_data2=[(19 ,50000, 18,'Sohan'),
(20 ,75000,  17,'Sima')]
wrong_column_data_schema2=['id','sal','age','name']
wrong_data_df=spark.createDataFrame(data=wrong_column_data2,schema=wrong_column_data_schema2)
wrong_data_df.show()

# COMMAND ----------

manager_df.unionByName(wrong_data_df).show()
manager_df.unionByName(wrong_data_df2).show()

# COMMAND ----------

# MAGIC %md
# MAGIC what if we add data with extra column, i.e mismatch of number of columns. Then how union will behave?
# MAGIC * it will give us error for column mismatch
# MAGIC * solution for this is to select some particular columns.

# COMMAND ----------

wrong_column_data3=[(19 ,50000, 18,'Sohan',10),
(20 ,75000,  17,'Sima',20)]
wrong_column_data_schema3=['id','sal','age','name','fake_id']
wrong_data_df3=spark.createDataFrame(data=wrong_column_data3,schema=wrong_column_data_schema3)
manager_df.union(wrong_data_df3).show()

# COMMAND ----------

wrong_data_df3.select('id','sal','age','name').union(manager_df).show()

# COMMAND ----------

