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

df = spark.table('catalog_earthquakes.default.homicide_victims_date_2025_09_17')

# COMMAND ----------

columns_list = {
    'Homicide ID': col('Homicide ID').cast(StringType()),
    'Count of Victims': col('Count of Victims').cast(IntegerType()),
    'Age Group': col('Age Group').cast(StringType()),
    'Sex': col('Sex').cast(StringType()),
    'Method of Killing': col('Method of Killing').cast(StringType()),
    'Domestic Abuse': col('Domestic Abuse').cast(IntegerType()), 
    'Recorded Date': col('Recorded Date').cast(StringType()), 
    'Homicide Offence Type': col('Homicide Offence Type').cast(StringType()), 
    'Solved Status': col('Solved Status').cast(StringType()), 
    'Borough': col('Borough').cast(StringType()), 
    'Officer Observed Ethnicity': col('Officer Observed Ethnicity').cast(StringType()), 
    }
df = df.withColumns(columns_list)

# COMMAND ----------

rename_cols = {
    'Homicide ID': 'homicide_id',
    'Count of Victims':'victim_count',
    'Age Group': 'age_group',
    'Sex': 'sex',
    'Method of Killing': 'used_method',
    'Domestic Abuse': 'domestic_abuse_flag',
    'Recorded Date': 'crime_recorded_date',
    'Homicide Offence Type':'homicide_offence_type',
    'Solved Status': 'solved_status',
    'Borough': 'crime_recorded_borough',
    'Officer Observed Ethnicity': 'officer_observed_ethnicity',
}
df = df.withColumnsRenamed(rename_cols)

# COMMAND ----------

df = df.withColumn('homicide_id', substring(col('homicide_id'), 15, 19))

# COMMAND ----------

df = df.withColumn('officer_observed_ethnicity', 
              when( col('officer_observed_ethnicity') == 'Not Reported/Not known', 'Unidentified')\
              .when( col('officer_observed_ethnicity') == 'Other.', 'Other')\
              .otherwise(col('officer_observed_ethnicity')))

# COMMAND ----------

df = df.withColumn('sex', 
              when( col('sex') == 'Unknown', 'Unidentified Victim')\
              .otherwise(col('sex')))

# COMMAND ----------

df = df.withColumn('used_method', 
              when( col('used_method') == 'Not known/Not Recorded', 'Unknown Method')\
              .otherwise(col('used_method')))

# COMMAND ----------

df = df.withColumn('crime_recorded_month', split(df.crime_recorded_date, '-').getItem(1)) \
       .withColumn('crime_recorded_year', concat(lit('20'), split(df.crime_recorded_date, '-').getItem(2)))

# COMMAND ----------

df = df.withColumn(
    "age_category",
    when(trim(col("age_group")) == "0 to 12", "Child")
    .when(trim(col("age_group")) == "13 to 19", "Teen")
    .when(trim(col("age_group")) == "20 to 24", "Young Adult")
    .when(trim(col("age_group")) == "25 to 34", "Adult")
    .when(trim(col("age_group")) == "35 to 44", "Adult")
    .when(trim(col("age_group")) == "45 to 54", "Middle-aged")
    .when(trim(col("age_group")) == "55 to 64", "Pre-senior")
    .when(trim(col("age_group")) == "65 and over", "Senior")
)

# COMMAND ----------

df = df.withColumn('is_solved_flag',when(col('solved_status')=='Solved', 1).otherwise(0))

# COMMAND ----------

df = df.drop('crime_recorded_date','officer_observed_ethnicity')

# COMMAND ----------

df.write.format('delta').save('/data/staging')

# COMMAND ----------

from datetime import date
current_date = date.today()
raw_file_path = f'/Volumes/workspace/default/data/raw_homicide_victims_date_{current_date}'

# COMMAND ----------

raw_file_path

# COMMAND ----------

df.write \
    .format('parquet')  \
    .mode('overwrite') \
    .save(raw_file_path)