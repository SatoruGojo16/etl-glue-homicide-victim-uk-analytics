# Databricks notebook source
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.transforms import *
from pyspark.sql.functions import * 
from pyspark.sql.types import * 
from pyspark.sql import SparkSession
from awsglue.utils import *
from awsglue.dynamicframe import DynamicFrame
import sys  
from datetime import datetime 
import pandas as pd

# COMMAND ----------

sc = SparkContext()
glueContext=GlueContext(sc)
spark=glueContext.spark_session

# COMMAND ----------


from pyspark.sql.functions import * 
from pyspark.sql.types import * 

# COMMAND ----------

from datetime import date
current_date = date.today().format("%Y-%m")
raw_file_path = f'/Volumes/workspace/default/data/raw_homicide_victims_date_{current_date}'

# COMMAND ----------

df = spark.read.parque(raw_file_path)

# COMMAND ----------

staging_case_details = df.select("homicide_id","victim_count","is_solved_flag","is_domestic_abuse_flag")
staging_case_details.write.mode('overwrite').saveAsTable('workspace.default.staging_case_details')

# COMMAND ----------

staging_age_group = df.select('age_group', 'age_category').distinct().orderBy('age_group')
staging_age_group.write.mode('overwrite').saveAsTable('workspace.default.staging_age_group')

# COMMAND ----------

staging_method_used = df.select(col('used_method').alias('method_description'), when(col('used_method').isin('Knife or Sharp Implement','Shooting','Blunt Implement'),1).otherwise(0).alias('is_object_used')).distinct().orderBy('method_description')
staging_method_used.write.mode('overwrite').saveAsTable('workspace.default.staging_method')

# COMMAND ----------

staging_gender_category = df.select(col('sex').alias('identified_sex')).distinct()
staging_gender_category.write.mode('overwrite').saveAsTable('workspace.default.staging_gender_category')

# COMMAND ----------

df_offense_type_lookup = spark.read.csv('/Volumes/workspace/default/data/homicide_offence_type.csv', header=True)
staging_offense_type = df_offense_type_lookup.select('offense_type', 'offense_type_law_description')
staging_offense_type.write.mode('overwrite').saveAsTable('workspace.default.staging_offense_type')

# COMMAND ----------

lookup_uk_population = spark.read.csv('/Volumes/workspace/default/data/london_population_dataset.txt', sep='|', header=True)
lookup_uk_population = lookup_uk_population.select('district','type','ceremonial county','region','population')
staging_location_lookup = lookup_uk_population.withColumn('population', regexp_replace(lookup_uk_population['population'], ',', ''))
staging_location_lookup_columns = [column.replace(' ', '_') for column in staging_location_lookup.columns]
staging_location_lookup = staging_location_lookup.toDF(*staging_location_lookup_columns)
staging_location_lookup.write.mode('overwrite').saveAsTable('workspace.default.staging_location')