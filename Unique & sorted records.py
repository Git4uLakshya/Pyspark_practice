# Databricks notebook source
# MAGIC %md
# MAGIC Topics to be covered
# MAGIC * how to find unique rows
# MAGIC * how to drop duplicate rows 
# MAGIC * how to sort the data in ascending and descending order
# MAGIC * 1 simple question of pyspark

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

data=[(10 ,'Anil',50000, 18),
(11 ,'Vikas',75000,  16),
(12 ,'Nisha',40000,  18),
(13 ,'Nidhi',60000,  17),
(14 ,'Priya',80000,  18),
(15 ,'Mohit',45000,  18),
(16 ,'Rajesh',90000, 10),
(17 ,'Raman',55000, 16),
(18 ,'Sam',65000,   17),
(15 ,'Mohit',45000,  18),
(13 ,'Nidhi',60000,  17),      
(14 ,'Priya',90000,  18),  
(18 ,'Sam',65000,   17)
     ]
schema=['id','name','sal','mngr_id']
manager_df=spark.createDataFrame(data=data,schema=schema)
manager_df.show()

# COMMAND ----------

manager_df.count()

# COMMAND ----------

manager_df.distinct().show()
manager_df.distinct().count()

# COMMAND ----------

manager_df.select('id','name').distinct().show()
manager_df.select('id','name').distinct().count()

# COMMAND ----------

#drop duplicates
dropped_mngr=manager_df.drop_duplicates(['id','name','sal','mngr_id'])
dropped_mngr.show()

# COMMAND ----------

#sorting 
manager_df.sort(col('sal').desc(),col('name').desc()).show()

# COMMAND ----------

#LeetCODE question 
data=[
    (1, 'Will', None),
    (2, 'Jane', None),
    (3, 'Alex', 2),
    (4, 'Bill', None),
    (5, 'Zack', 1),
    (6, 'Mark', 2)
]
schema=['id','name','refree_id']
customer=spark.createDataFrame(data=data,schema=schema)
customer.show()

# COMMAND ----------

customer.select(col('name')).filter((col('refree_id') !=2) | (col('refree_id').isNull())).show()
#notes
#isnull() to handle null values
#use | ,& in place of or and and
#use proper () for filters

# COMMAND ----------

