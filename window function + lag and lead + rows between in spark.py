# Databricks notebook source
# MAGIC %md
# MAGIC Topics to be covered
# MAGIC * understanding of window function 
# MAGIC * Row number, Rank and dense rank 
# MAGIC * Calculating top 2 male and female earner from each department 

# COMMAND ----------

# MAGIC %md
# MAGIC #### General structure of window function 
# MAGIC from pyspark.sql.window import Window
# MAGIC
# MAGIC
# MAGIC var_window= Window.partitionBy('col').orderBy('col')
# MAGIC
# MAGIC
# MAGIC your_df=your_df.withColumn('column_name',window_function().over(var_window))
# MAGIC
# MAGIC
# MAGIC your_df.show()
# MAGIC
# MAGIC #### Lag or Lead takes 3 argument. column name from which record to be fetch, number of record lag or lead, default values
# MAGIC ### Rows Between
# MAGIC * we have first and last function as a Window function which will give you the first and last value of a given column in a window frame.
# MAGIC * By default, for a window, frame is unbounding preceeding and current row. 
# MAGIC * But you can change that to unbounded preceeding or unbounded following or current row or any fixed number of rows as a preceeding or following or to a range of values
# MAGIC * Rows between takes 2 argument. one for preceeding and other for following. 

# COMMAND ----------

# MAGIC %md
# MAGIC #####Window Function

# COMMAND ----------

emp_data = [(1,'manish',50000,'IT','m'),
(2,'vikash',60000,'sales','m'),
(3,'raushan',70000,'marketing','m'),
(4,'mukesh',80000,'IT','m'),
(5,'priti',90000,'sales','f'),
(6,'nikita',45000,'marketing','f'),
(7,'ragini',55000,'marketing','f'),
(8,'rashi',100000,'IT','f'),
(9,'aditya',65000,'IT','m'),
(10,'rahul',50000,'marketing','m'),
(11,'rakhi',50000,'IT','f'),
(12,'akhilesh',90000,'sales','m')]
emp_schema = ['id','name','salary', 'dept', 'gender']
emp_df=spark.createDataFrame(data=emp_data,schema=emp_schema)
emp_df.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

window= Window.partitionBy('dept','gender').orderBy(desc('salary'))
emp_df.withColumn('Row_number',row_number().over(window))\
    .withColumn('Rank',rank().over(window))\
    .withColumn('DenseRank',dense_rank().over(window))\
    .filter(col('DenseRank')<=2)\
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC #####LAG or LEAD

# COMMAND ----------

# MAGIC %md
# MAGIC what is the % of sales each month based on last 6 months sales.

# COMMAND ----------

product_data = [(1,"iphone","01-01-2023",1500000),
(2,"samsung","01-01-2023",1100000),
(3,"oneplus","01-01-2023",1100000),
(1,"iphone","01-02-2023",1300000),
(2,"samsung","01-02-2023",1120000),
(3,"oneplus","01-02-2023",1120000),
(1,"iphone","01-03-2023",1600000),
(2,"samsung","01-03-2023",1080000),
(3,"oneplus","01-03-2023",1160000),
(1,"iphone","01-04-2023",1700000),
(2,"samsung","01-04-2023",1800000),
(3,"oneplus","01-04-2023",1170000),
(1,"iphone","01-05-2023",1200000),
(2,"samsung","01-05-2023",980000),
(3,"oneplus","01-05-2023",1175000),
(1,"iphone","01-06-2023",1100000),
(2,"samsung","01-06-2023",1100000),
(3,"oneplus","01-06-2023",1200000)
]
product_schema=['Product_id','Product_name','Sale_date','sales']
product_df=spark.createDataFrame(data=product_data,schema=product_schema)
product_df.show()

# COMMAND ----------

product_window = Window.partitionBy('Product_name').orderBy('Sale_date')
product_df.withColumn('total_sale', sum(col('sales')).over(product_window))\
            .withColumn('percentage',round((col('total_sale')-col('sales'))*100/col('total_sale'),2))\
            .show()

# COMMAND ----------

# MAGIC %md
# MAGIC what is the % of loss or gain based on the previous month sales?

# COMMAND ----------

product_window = Window.partitionBy('Product_name').orderBy('Sale_date')
product_df.withColumn('Previous_month_sale', lag(col('sales'),1).over(product_window))\
            .withColumn('Gain/Loss',round((col('sales')-col('Previous_month_sale'))*100/col('sales'),2))\
            .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Rows Between

# COMMAND ----------

# MAGIC %md
# MAGIC Ques 1: Find out the difference in sales, of each product from their first month sales to latest sales?

# COMMAND ----------

product_data = [(2,"samsung","01-01-1995",11000),
(1,"iphone","01-02-2023",1300000),
(2,"samsung","01-02-2023",1120000),
(3,"oneplus","01-02-2023",1120000),
(1,"iphone","01-03-2023",1600000),
(2,"samsung","01-03-2023",1080000),
(3,"oneplus","01-03-2023",1160000),
(1,"iphone","01-01-2006",15000),
(1,"iphone","01-04-2023",1700000),
(2,"samsung","01-04-2023",1800000),
(3,"oneplus","01-04-2023",1170000),
(1,"iphone","01-05-2023",1200000),
(2,"samsung","01-05-2023",980000),
(3,"oneplus","01-05-2023",1175000),
(1,"iphone","01-06-2023",1100000),
(3,"oneplus","01-01-2010",23000),
(2,"samsung","01-06-2023",1100000),
(3,"oneplus","01-06-2023",1200000)
]

product_schema=["product_id","product_name","sales_date","sales"]

product_df = spark.createDataFrame(data=product_data,schema=product_schema)

product_df.show()

# COMMAND ----------

q1window=Window.partitionBy('product_name').orderBy('sales_date')
sales_df= product_df.withColumn('Rank',rank().over(q1window)).filter(col('Rank')==1).select('product_name','sales')
product_df.alias('p').join(sales_df.alias('s'),col('p.product_name')==col('s.product_name'))\
            .withColumn('diff',col('p.sales')-col('s.sales'))\
            .select("product_id",'p.product_name',"sales_date",'p.sales','s.sales','diff')\
            .orderBy('p.product_name','sales_date').show()

# COMMAND ----------

# MAGIC %md
# MAGIC OR You can do it like below

# COMMAND ----------

window= Window.partitionBy('product_name').rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing).orderBy('sales_date')
product_df.withColumn('First',first('sales').over(window)).show()

# COMMAND ----------

