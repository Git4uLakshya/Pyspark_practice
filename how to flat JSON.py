# Databricks notebook source
rest_JSON=spark.read.format('JSON')\
        .option('Multiline','True')\
        .option('inferSchema','True')\
        .load('/FileStore/tables/resturant_json_data.json')

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

rest_JSON.show()

# COMMAND ----------

rest_JSON.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC * when datatype of a column is ARRAY: you need to use EXPLODE to flat a JSON. It will make array to struct.
# MAGIC   * if data in explode column is null then it will drop all the data. To resolve this issue we need to use explode_outer.
# MAGIC * when datatype of a column is STRUCT: you can access the column using .keyword.

# COMMAND ----------

# datatype Array
rest_JSON.select('*',explode('restaurants').alias('new_rest')).drop('restaurants').printSchema()
# Here you have selected all the columns and exploded restaurant column. You can explode any other column with restaurant if that is on the same level as restaurants. 
# you dropped the column restaurant coz it is coming along with column we get get after explode i.e new_rest.

# COMMAND ----------

# DataType struct
rest_JSON.select('*',explode('restaurants').alias('new_rest')).drop('restaurants')\
        .select('*','new_rest.restaurant.r.res_id').printSchema()

# COMMAND ----------

# suppose we want res_id, establishment_types.element, offers.element, name
rest_JSON.select('*',explode('restaurants').alias('new_rest'))\
            .select('*','new_rest.restaurant.r.res_id')\
            .select('*',explode_outer('new_rest.restaurant.establishment_types').alias('new_establishment_types'))\
            .select('*','new_rest.restaurant.name')\
            .select('*',explode_outer('new_rest.restaurant.offers').alias('new_offers'))\
                .drop('restaurants','code','message','results_found','results_shown','results_start','status','new_rest').show()

# COMMAND ----------

