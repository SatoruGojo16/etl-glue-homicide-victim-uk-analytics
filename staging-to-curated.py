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
from geopy.geocoders import Nominatim

# COMMAND ----------

sc = SparkContext()
glueContext=GlueContext(sc)
spark=glueContext.spark_session

# COMMAND ----------

if not spark.catalog.tableExists('workspace.default.dim_date'):
    dim_date = spark.sql('''
    SELECT explode(sequence(to_date('2000-01-01'), to_date('2030-01-01'), interval 1 months)) as date
    ''')
    dim_date_cols = {
    "date_id":date_format(dim_date.date,'MMdd'),
    "month":date_format(dim_date.date, 'M'),
    "month_short":date_format(dim_date.date, "LLL"), 
    "month_long":date_format(dim_date.date, "LLLL"),
    "year_short":date_format(dim_date.date, 'yy'),
    "year_long":date_format(dim_date.date, 'yyyy'),
    "quarter":ceil(date_format(dim_date.date, 'M')/3),
    }
    dim_date = dim_date.withColumns(dim_date_cols)
    dim_date = dim_date.drop('date')
    dim_date.write.mode('overwrite').saveAsTable('workspace.default.dim_date') 

# COMMAND ----------

coords_strct = StructType([
    StructField("latitide", StringType(), True),
    StructField("longitude", StringType(), True)
    ])
@udf(coords_strct)
def udf_get_coords(address):
    try:
        geolocator = Nominatim(user_agent="homicide_victims_dataset_geolocator")
        location = geolocator.geocode(address)
        return location.latitude, location.longitude
    except:
        return None, None

# COMMAND ----------

if not spark.catalog.tableExists('workspace.default.dim_location'):
    staging_location = spark.read.table('workspace.default.staging_location')
    staging_location = staging_location.withColumn('coordinates', udf_get_coords(concat(col('district'),lit(','),col('region'))))
    staging_location = staging_location.withColumn('latitude', staging_location.coordinates.latitide)
    staging_location = staging_location.withColumn('longitude', staging_location.coordinates.longitude)
    staging_location = staging_location.drop('coordinates')
    staging_location.write.mode('overwrite').saveAsTable('workspace.default.dim_location')

# COMMAND ----------

if spark.catalog.tableExists('workspace.default.dim_case_details'):
    spark.sql(""" 
            MERGE INTO workspace.default.dim_case_details AS target
            USING workspace.default.staging_case_details AS source
            ON target.homicide_id = source.homicide_id
            WHEN MATCHED THEN
            UPDATE SET target.victim_count = source.victim_count,
            target.is_solved_flag = source.is_solved_flag,
            target.is_domestic_abuse_flag = source.is_domestic_abuse_flag
            WHEN NOT MATCHED THEN
            INSERT (homicide_id, victim_count, is_solved_flag, is_domestic_abuse_flag)
            VALUES (source.homicide_id, source.victim_count, source.is_solved_flag, source.is_domestic_abuse_flag)
            """)
else: 
    spark.sql("""
              CREATE TABLE workspace.default.dim_case_details(
                  homicide_id string,
                  victim_count integer,
                  is_solved_flag integer,
                  is_domestic_abuse_flag integer
              )   
              """)
    spark.read.table('workspace.default.staging_case_details').writeTo('workspace.default.dim_case_details').append()

# COMMAND ----------

if spark.catalog.tableExists('workspace.default.dim_age_group'):
    spark.sql(""" 
            MERGE INTO workspace.default.dim_age_group AS target
            USING workspace.default.staging_age_group  AS source
            ON target.age_group = source.age_group
            WHEN NOT MATCHED THEN
            INSERT (age_group, age_category)
            VALUES (source.age_group, source.age_category)
            """)
else: 
    spark.sql("""
              CREATE TABLE workspace.default.dim_age_group(
                  age_group_id long generated always as identity,
                  age_group string,
                  age_category string
              )  
              """)
    spark.read.table('workspace.default.staging_age_group').writeTo('workspace.default.dim_age_group').append()


# COMMAND ----------

if spark.catalog.tableExists('workspace.default.dim_method'):
    spark.sql(""" 
            MERGE INTO workspace.default.dim_method AS target
            USING workspace.default.staging_method AS source
            ON target.method_description = source.method_description
            WHEN NOT MATCHED THEN
            INSERT (method_description, is_object_used)
            VALUES (source.method_description, source.is_object_used)
            """)
else: 
    spark.sql("""
              CREATE TABLE workspace.default.dim_method(
                  method_id long generated always as identity,
                  method_description string,
                  is_object_used INTEGER
              )  
              """)
    spark.read.table('workspace.default.staging_method').writeTo('workspace.default.dim_method').append()

# COMMAND ----------

if spark.catalog.tableExists('workspace.default.dim_gender_category'):
    spark.sql(""" 
            MERGE INTO workspace.default.staging_gender_category AS target
            USING workspace.default.dim_gender_category AS source
            ON target.identified_sex = source.identified_sex
            WHEN NOT MATCHED THEN
            INSERT (identified_sex)
            VALUES (source.identified_sex)
            """)
else:
    spark.sql("""
              CREATE TABLE workspace.default.dim_gender_category(
                  gender_id long generated always as identity,
                  identified_sex string
              )  
              """)
    spark.read.table('workspace.default.staging_gender_category').writeTo('workspace.default.dim_gender_category').append()

# COMMAND ----------


if spark.catalog.tableExists('workspace.default.dim_offense_type'):
    spark.sql(""" 
            MERGE INTO workspace.default.staging_offense_type AS target
            USING workspace.default.dim_offense_type AS source
            ON target.offense_type = source.offense_type
            WHEN NOT MATCHED THEN
            INSERT (offense_type, offense_type_law_description)
            VALUES (source.offense_type, source.offense_type_law_description)
            """)
else:
    spark.sql("""
              CREATE TABLE workspace.default.dim_offense_type(
                  offense_type_id long generated always as identity,
                  offense_type string,
                  offense_type_law_description string
              )  
              """)
    spark.read.table('workspace.default.staging_offense_type').writeTo('workspace.default.dim_offense_type').append()

# COMMAND ----------

from datetime import date
current_date = date.today().strftime("%Y-%m")
raw_file_path = f'/Volumes/workspace/default/data/raw_homicide_victims_date_{current_date}.parquet'
raw_df = spark.read.parque(raw_file_path)

# COMMAND ----------

dim_case_details = spark.read.table("workspace.default.dim_case_details")
dim_age_group = spark.read.table("workspace.default.dim_age_group")
dim_method = spark.read.table("workspace.default.dim_method")
dim_gender_category = spark.read.table("workspace.default.dim_gender_category")
dim_offense_type = spark.read.table("workspace.default.dim_offense_type")
dim_location = spark.read.table("workspace.default.dim_location")
dim_date = spark.read.table("workspace.default.dim_date")

# COMMAND ----------

fact_homicide_cases = raw_df.join(dim_case_details, "homicide_id") \
                .join(dim_age_group, "age_group") \
                .join(dim_method, on = (raw_df["used_method"] == dim_method["method_description"])) \
                .join(dim_gender_category, on = (raw_df['sex'] == dim_gender_category["identified_sex"])) \
                .join(dim_offense_type, dim_offense_type["offense_type"] == raw_df["homicide_offence_type"]) \
                .join(dim_date, on = ((raw_df["crime_recorded_year"] == dim_date["year_long"]) & (raw_df['crime_recorded_month'] == dim_date['month_short']))) \
                .withColumn('load_date',lit(date_format(current_date(),'yMM')))
fact_homicide_cases = fact_homicide_cases.select('homicide_id','age_group_id','method_id','gender_category_id','offense_type_id','date_id','load_date')

# COMMAND ----------

if not spark.catalog.tableExists('workspace.default.fact_homicide_cases'):
    fact_homicide_cases.writeTo('workspace.default.fact_homicide_cases').partitionedBy('load_date').createOrReplace()
else:
    fact_homicide_cases.writeTo('workspace.default.fact_homicide_cases').partitionedBy('load_date').append()