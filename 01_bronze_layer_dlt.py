# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Raw Data Ingestion
# MAGIC
# MAGIC This notebook handles the ingestion of raw healthcare data into the Bronze layer using Delta Live Tables.

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Claims Batch Data Ingestion

# COMMAND ----------

@dlt.table(
    name="claims_batch",
    comment="Raw claims batch data ingested with Auto Loader",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def bronze_claims_batch():
    claims_batch_schema = StructType([
        StructField("ClaimID", StringType(), False),
        StructField("MemberID", StringType(), False),
        StructField("ProviderID", StringType(), False),
        StructField("ClaimDate", StringType(), True),
        StructField("ServiceDate", StringType(), True),
        StructField("Amount", DecimalType(10,2), True),
        StructField("Status", StringType(), True),
        StructField("ICD10Codes", StringType(), True),
        StructField("CPTCodes", StringType(), True),
        StructField("ClaimType", StringType(), True),
        StructField("SubmissionChannel", StringType(), True),
        StructField("Notes", StringType(), True),
        StructField("IngestTimestamp", StringType(), True)
    ])

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(claims_batch_schema)
        .load("/Volumes/capstone/bronze/raw_files/claims_batch/")
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("ClaimDate", to_date(col("ClaimDate"), "yyyy-MM-dd"))
        .withColumn("ServiceDate", to_date(col("ServiceDate"), "yyyy-MM-dd"))
        .withColumn("OriginalIngestTimestamp", to_timestamp(col("IngestTimestamp"), "yyyy-MM-dd HH:mm:ss"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Claims Streaming Data Ingestion

# COMMAND ----------

@dlt.table(
    name="claims_stream",
    comment="Raw streaming claims data ingested with Auto Loader",
    table_properties={
        "quality": "bronze"
    }
)
def bronze_claims_stream():
    streaming_schema = StructType([
        StructField("ClaimID", StringType(), False),
        StructField("MemberID", StringType(), False),
        StructField("ProviderID", StringType(), False),
        StructField("ClaimDate", StringType(), True),
        StructField("Amount", DecimalType(10,2), True),
        StructField("Status", StringType(), True),
        StructField("ICD10Codes", StringType(), True),
        StructField("CPTCodes", StringType(), True),
        StructField("EventTimestamp", StringType(), True)
    ])

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(streaming_schema)
        .load("/Volumes/capstone/bronze/raw_files/claims_stream/")
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("ClaimDate", to_date(col("ClaimDate"), "yyyy-MM-dd"))
        .withColumn("EventTimestamp", to_timestamp(col("EventTimestamp"), "yyyy-MM-dd HH:mm:ss"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Member Data Ingestion

# COMMAND ----------

@dlt.table(
    name="members",
    comment="Raw member data from CSV files",
    table_properties={
        "quality": "bronze"
    }
)
def bronze_members():
    """
    Ingest raw member data from CSV files
    """
    members_schema = StructType([
        StructField("MemberID", StringType(), False),
        StructField("Name", StringType(), True),
        StructField("DOB", StringType(), True),
        StructField("Gender", StringType(), True),
        StructField("Region", StringType(), True),
        StructField("PlanType", StringType(), True),
        StructField("EffectiveDate", StringType(), True),
        StructField("Email", StringType(), True),
        StructField("IsActive", DoubleType(), True),
        StructField("LastUpdated", StringType(), True)
    ])
    
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(members_schema)
        .load("/Volumes/capstone/bronze/raw_files/members.csv")
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("DOB", to_date(col("DOB"), "yyyy-MM-dd"))
        .withColumn("EffectiveDate", to_date(col("EffectiveDate"), "yyyy-MM-dd"))
        .withColumn("LastUpdated", to_date(col("LastUpdated"), "yyyy-MM-dd"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Provider Data Ingestion

# COMMAND ----------

@dlt.table(
    name="providers",
    comment="Raw provider data from JSON files",
    table_properties={
        "quality": "bronze"
    }
)
def bronze_providers():
    """
    Ingest raw provider data from JSON files
    """
    provider_schema = StructType([
        StructField("ProviderID", StringType(), False),
        StructField("Name", StringType(), True),
        StructField("Specialties", ArrayType(StringType()), True),
        StructField("Locations", ArrayType(StructType([
            StructField("Address", StringType(), True),
            StructField("City", StringType(), True),
            StructField("State", StringType(), True)
        ])), True),
        StructField("IsActive", BooleanType(), True),
        StructField("TIN", StringType(), True),
        StructField("LastVerified", StringType(), True)
    ])
    
    return (
        spark.read
        .format("json")
        .schema(provider_schema)
        .load("/Volumes/capstone/bronze/raw_files/providers.json")
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("LastVerified", to_date(col("LastVerified"), "yyyy-MM-dd"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Diagnosis Reference Data Ingestion

# COMMAND ----------

@dlt.table(
    name="diagnosis_reference",
    comment="Raw diagnosis reference data from CSV files",
    table_properties={
        "quality": "bronze"
    }
)
def bronze_diagnosis_reference():
    """
    Ingest raw diagnosis reference data from CSV files
    """
    diagnosis_schema = StructType([
        StructField("Code", StringType(), False),
        StructField("Description", StringType(), True)
    ])
    
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(diagnosis_schema)
        .load("/Volumes/capstone/bronze/raw_files/diagnosis_ref.csv")
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Validation Summary
# MAGIC
# MAGIC The Bronze layer focuses on:
# MAGIC - Raw data ingestion with minimal transformation
# MAGIC - Schema enforcement
# MAGIC - Adding metadata columns (ingestion timestamp, source file)
# MAGIC - Basic date parsing for downstream processing
# MAGIC - Maintaining data lineage information