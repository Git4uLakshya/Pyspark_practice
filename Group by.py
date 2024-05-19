# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

emp_data=[(1,'manish',50000,'IT'),
(2,'vikash',60000,'sales'),
(3,'raushan',70000,'marketing'),
(4,'mukesh',80000,'IT'),
(5,'pritam',90000,'sales'),
(6,'nikita',45000,'marketing'),
(7,'ragini',55000,'marketing'),
(8,'rakesh',100000,'IT'),
(9,'aditya',65000,'IT'),
(10,'rahul',50000,'marketing')]
emp_schema=['id','name','salary', 'dept']
emp_df=spark.createDataFrame(data=emp_data,schema=emp_schema)
emp_df.show()

# COMMAND ----------

emp_df.groupBy('dept')\
        .agg(sum('salary')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC spark sql

# COMMAND ----------

emp_df.createOrReplaceTempView('emp_table')

# COMMAND ----------

spark.sql("""
          select dept, sum(salary)
          from emp_table
          group by dept
          """).show()

# COMMAND ----------

# MAGIC %md 
# MAGIC question: find total salary in each dept in each country

# COMMAND ----------

emp_data1=[(1,'manish',50000,'IT','india'),
(2,'vikash',60000,'sales','us'),
(3,'raushan',70000,'marketing','india'),
(4,'mukesh',80000,'IT','us'),
(5,'pritam',90000,'sales','india'),
(6,'nikita',45000,'marketing','us'),
(7,'ragini',55000,'marketing','india'),
(8,'rakesh',100000,'IT','us'),
(9,'aditya',65000,'IT','india'),
(10,'rahul',50000,'marketing','us')]
emp_schema1=['id','name','salary','dept','counrty']
emp_df1=spark.createDataFrame(data=emp_data1,schema=emp_schema1)
emp_df1.show()

# COMMAND ----------

emp_df1.groupBy('counrty','dept')\
        .agg(sum('salary')).show()

# COMMAND ----------

