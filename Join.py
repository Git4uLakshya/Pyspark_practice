# Databricks notebook source
# MAGIC %md
# MAGIC Syntax of Join
# MAGIC
# MAGIC dataframe1.join(dataframe2, join_expression, join_type)
# MAGIC
# MAGIC * join_expression= expression for column on which the join operation is performed.
# MAGIC * join type: The type of join to be performed, such as 'inner', 'outer', 'left', or 'right'.

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

customer_data = [(1,'manish','patna',"30-05-2022"),
(2,'vikash','kolkata',"12-03-2023"),
(3,'nikita','delhi',"25-06-2023"),
(4,'rahul','ranchi',"24-03-2023"),
(5,'mahesh','jaipur',"22-03-2023"),
(6,'prantosh','kolkata',"18-10-2022"),
(7,'raman','patna',"30-12-2022"),
(8,'prakash','ranchi',"24-02-2023"),
(9,'ragini','kolkata',"03-03-2023"),
(10,'raushan','jaipur',"05-02-2023")]

customer_schema=['customer_id','customer_name','address','date_of_joining']
customer_df= spark.createDataFrame(data=customer_data,schema=customer_schema)


sales_data = [(1,22,10,"01-06-2022"),
(1,27,5,"03-02-2023"),
(2,5,3,"01-06-2023"),
(5,22,1,"22-03-2023"),
(7,22,4,"03-02-2023"),
(9,5,6,"03-03-2023"),
(2,1,12,"15-06-2023"),
(1,56,2,"25-06-2023"),
(5,12,5,"15-04-2023"),
(11,12,76,"12-03-2023")]

sales_schema=['customer_id','product_id','quantity','date_of_purchase']

sales_df= spark.createDataFrame(data=sales_data,schema=sales_schema)


product_data = [(1, 'fanta',20),
(2, 'dew',22),
(5, 'sprite',40),
(7, 'redbull',100),
(12,'mazza',45),
(22,'coke',27),
(25,'limca',21),
(27,'pepsi',14),
(56,'sting',10)]

product_schema=['id','name','price']

product_df= spark.createDataFrame(data=product_data,schema=product_schema)


# COMMAND ----------

customer_df.show()

# COMMAND ----------

sales_df.show()

# COMMAND ----------

customer_df.join(sales_df,sales_df['customer_id']==customer_df['customer_id'],'inner').show()

# COMMAND ----------

customer_df.join(sales_df,sales_df['customer_id']==customer_df['customer_id'],'inner')\
            .select(customer_df['customer_id']).distinct().show()

# COMMAND ----------

