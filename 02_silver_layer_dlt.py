# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Data Cleaning and Processing
# MAGIC
# MAGIC This notebook handles data cleaning, standardization, and quality checks for the Silver layer using Delta Live Tables.

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Claims Processing with Data Quality Checks

# COMMAND ----------

@dlt.table(
    name="claims_processed",
    comment="Cleaned and standardized claims data with quality checks",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_all_or_drop({
    "valid_claim_id": "claim_id IS NOT NULL",
    "valid_member_id": "member_id IS NOT NULL", 
    "valid_provider_id": "provider_id IS NOT NULL",
    "positive_amount": "claim_amount > 0",
    "valid_service_date": "service_date IS NOT NULL AND service_date <= current_date()"
})
def silver_claims_processed():
    """
    Process and clean claims data from both batch and streaming sources
    Apply data quality rules and standardization
    """
    # Read from Bronze tables
    batch_claims = spark.table("capstone.bronze.claims_batch")
    
    # Try to read streaming claims, handle gracefully if not exists
    try:
        streaming_claims = spark.table("capstone.bronze.claims_stream")
        # Check if streaming data actually has records
        streaming_count = streaming_claims.count()
        has_streaming = streaming_count > 0
    except:
        has_streaming = False
    
    # Standardize batch claims structure
    batch_standardized = batch_claims.select(
        col("ClaimID").alias("claim_id"),
        col("MemberID").alias("member_id"),
        col("ProviderID").alias("provider_id"),
        col("Amount").alias("claim_amount"),
        col("ClaimDate").alias("claim_date"),
        col("ServiceDate").alias("service_date"),
        col("Status").alias("status"),
        col("ICD10Codes").alias("icd10_codes"),
        col("CPTCodes").alias("cpt_codes"),
        col("ClaimType").alias("claim_type"),
        col("SubmissionChannel").alias("submission_channel"),
        col("Notes").alias("notes"),
        col("OriginalIngestTimestamp").alias("original_ingest_timestamp"),
        col("ingestion_timestamp"),
        col("source_file"),
        lit("batch").alias("source_type")
    )
    
    if has_streaming:
        # Standardize streaming claims structure
        streaming_standardized = streaming_claims.select(
            col("ClaimID").alias("claim_id"),
            col("MemberID").alias("member_id"),
            col("ProviderID").alias("provider_id"),
            col("Amount").alias("claim_amount"),
            col("ClaimDate").alias("claim_date"),
            col("ClaimDate").alias("service_date"),  # ServiceDate missing in streaming
            col("Status").alias("status"),
            col("ICD10Codes").alias("icd10_codes"),
            col("CPTCodes").alias("cpt_codes"),
            lit("Outpatient").alias("claim_type"),     # Default for streaming
            lit("API").alias("submission_channel"),    # Default for streaming
            lit(None).cast("string").alias("notes"),   # Not available in streaming
            col("EventTimestamp").alias("original_ingest_timestamp"),
            col("ingestion_timestamp"),
            col("source_file"),
            lit("streaming").alias("source_type")
        )
        
        # Union both claim sources
        all_claims = batch_standardized.union(streaming_standardized)
    else:
        all_claims = batch_standardized
    
    # Apply data quality transformations and business rules
    quality_claims = (all_claims
        # Split diagnosis and procedure codes into arrays
        .withColumn("icd10_codes_array", 
                    when(col("icd10_codes").isNotNull(), split(col("icd10_codes"), ";"))
                    .otherwise(array()))
        .withColumn("cpt_codes_array",
                    when(col("cpt_codes").isNotNull(), split(col("cpt_codes"), ";"))
                    .otherwise(array()))
        
        # Add business rule indicators
        .withColumn("is_high_value_claim", col("claim_amount") > 5000)
        .withColumn("has_multiple_diagnoses", size(col("icd10_codes_array")) > 1)
        .withColumn("has_multiple_procedures", size(col("cpt_codes_array")) > 1)
        
        # Calculate processing metrics
        .withColumn("days_to_claim", 
                    datediff(col("claim_date"), col("service_date")))
        
        # Data validation flags
        .withColumn("is_valid_claim", 
                    when(col("claim_amount") > 50000, False)  # Suspiciously high amount
                    .when(col("days_to_claim") < 0, False)    # Future service date
                    .when(col("days_to_claim") > 365, False)  # Very old claim
                    .when(col("icd10_codes").isNull(), False) # Missing diagnosis
                    .otherwise(True))
        
        # Add processing metadata
        .withColumn("processed_timestamp", current_timestamp())
    )
    
    # Deduplication strategy: keep most recent record by ingestion timestamp
    window_spec = Window.partitionBy("claim_id").orderBy(desc("ingestion_timestamp"))
    
    return (quality_claims
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from capstone.silver.claims_processed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Member Data Processing with PII Protection

# COMMAND ----------

@dlt.table(
    name="members_processed",
    comment="Cleaned member data with PII masking and quality checks",
    table_properties={
        "quality": "silver"
    }
)
@dlt.expect_all_or_drop({
    "valid_member_id": "MemberID IS NOT NULL",
    "member_id_length": "length(MemberID) >= 5"
})
def silver_members_processed():
    """
    Process member data with PII protection and data quality checks
    """
    members_raw = spark.table("capstone.bronze.members")
    
    processed_members = (members_raw
        # PII Protection: Mask email addresses
        .withColumn("masked_email", 
                    when(col("Email").isNotNull(),
                         regexp_replace(col("Email"), "(.{3}).*@", "$1***@"))
                    .otherwise(lit(None)))
        
        # Business calculations
        .withColumn("age", 
                    when(col("DOB").isNotNull(),
                         floor(datediff(current_date(), col("DOB")) / 365.25))
                    .otherwise(lit(None)))
        
        # Standardize active status (convert numeric to boolean)
        .withColumn("is_active_member",
                    when(col("IsActive") == 1.0, True)
                    .when(col("IsActive") == 0.0, False)
                    .otherwise(lit(None)))
        
        # Data quality indicators
        .withColumn("has_complete_profile",
                    when(col("Name").isNotNull() & 
                         col("DOB").isNotNull() & 
                         col("Gender").isNotNull() & 
                         col("Email").isNotNull(), True)
                    .otherwise(False))
        
        # Add processing metadata
        .withColumn("processed_timestamp", current_timestamp())
    )
    
    # Deduplication: keep most recent record per member
    window_spec = Window.partitionBy("MemberID").orderBy(col("LastUpdated").desc_nulls_last())
    
    return (processed_members
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Provider Data Normalization

# COMMAND ----------

@dlt.table(
    name="providers_processed", 
    comment="Normalized provider data with exploded specialties and locations",
    table_properties={
        "quality": "silver"
    }
)
@dlt.expect_all_or_drop({
    "valid_provider_id": "provider_id IS NOT NULL",
    "valid_provider_name": "provider_name IS NOT NULL"
})
def silver_providers_processed():
    """
    Normalize provider data by exploding nested arrays
    Apply data standardization and quality checks
    """
    providers_raw = spark.table("capstone.bronze.providers")
    
    # First deduplicate at provider level (keep most recent)
    window_spec = Window.partitionBy("ProviderID").orderBy(desc("LastVerified"))
    
    providers_dedup = (providers_raw
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )
    
    # Normalize nested structure: explode arrays to create one row per specialty/location combination
    providers_normalized = (providers_dedup
        .withColumn("specialty", explode_outer(col("Specialties")))
        .withColumn("location", explode_outer(col("Locations")))
        .select(
            col("ProviderID").alias("provider_id"),
            col("Name").alias("provider_name"),
            col("specialty"),
            col("location.Address").alias("address"),
            col("location.City").alias("city"),
            col("location.State").alias("state"),
            col("IsActive").alias("is_active"),
            col("TIN").alias("tin"),
            col("LastVerified").alias("last_verified"),
            col("ingestion_timestamp")
        )
        
        # Data standardization
        .withColumn("state_standardized", 
                    when(col("state").isNotNull(), upper(trim(col("state"))))
                    .otherwise(lit(None)))
        
        .withColumn("specialty_standardized",
                    when(col("specialty").isNotNull(), trim(col("specialty")))
                    .otherwise(lit(None)))
        
        # Data quality indicators
        .withColumn("has_complete_address",
                    when(col("address").isNotNull() & 
                         col("city").isNotNull() & 
                         col("state").isNotNull(), True)
                    .otherwise(False))
        
        .withColumn("processed_timestamp", current_timestamp())
    )
    
    return providers_normalized

# COMMAND ----------

# MAGIC %md
# MAGIC ## Diagnosis Reference Processing

# COMMAND ----------

@dlt.table(
    name="diagnosis_reference_processed",
    comment="Cleaned diagnosis reference data",
    table_properties={
        "quality": "silver"
    }
)
@dlt.expect_all_or_drop({
    "valid_diagnosis_code": "Code IS NOT NULL",
    "valid_diagnosis_description": "Description IS NOT NULL"
})
def silver_diagnosis_reference_processed():
    """
    Process diagnosis reference data with standardization
    """
    diagnosis_raw = spark.table("capstone.bronze.diagnosis_reference")
    
    return (diagnosis_raw
        .withColumn("code_standardized", upper(trim(col("Code"))))
        .withColumn("description_clean", trim(col("Description")))
        .withColumn("processed_timestamp", current_timestamp())
        # Deduplicate by code
        .dropDuplicates(["Code"])
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Summary Views

# COMMAND ----------

@dlt.view(
    name="data_quality_summary",
    comment="Summary of data quality metrics across all Silver tables"
)
def silver_data_quality_summary():
    """
    Create a summary view of data quality metrics
    """
    claims = spark.table("capstone.silver.claims_processed")
    members = spark.table("capstone.silver.members_processed") 
    providers = spark.table("capstone.silver.providers_processed")
    
    # Claims quality metrics
    claims_quality = claims.agg(
        lit("claims").alias("table_name"),
        count("*").alias("total_records"),
        sum(when(col("is_valid_claim"), 1).otherwise(0)).alias("valid_records"),
        sum(when(col("is_high_value_claim"), 1).otherwise(0)).alias("high_value_claims"),
        current_timestamp().alias("calculated_at")
    )
    
    # Members quality metrics  
    members_quality = members.agg(
        lit("members").alias("table_name"),
        count("*").alias("total_records"),
        sum(when(col("has_complete_profile"), 1).otherwise(0)).alias("valid_records"),
        sum(when(col("is_active_member"), 1).otherwise(0)).alias("active_members"),
        current_timestamp().alias("calculated_at")
    )
    
    # Providers quality metrics
    providers_quality = providers.agg(
        lit("providers").alias("table_name"), 
        count("*").alias("total_records"),
        sum(when(col("has_complete_address"), 1).otherwise(0)).alias("valid_records"),
        sum(when(col("is_active"), 1).otherwise(0)).alias("active_providers"),
        current_timestamp().alias("calculated_at")
    )
    
    return claims_quality.union(members_quality).union(providers_quality)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer Summary
# MAGIC
# MAGIC The Silver layer provides:
# MAGIC - **Data Quality Enforcement**: Using DLT expectations to ensure data meets business rules
# MAGIC - **Standardization**: Consistent naming, formatting, and data types
# MAGIC - **PII Protection**: Masking sensitive information like email addresses
# MAGIC - **Business Logic**: Adding calculated fields and indicators
# MAGIC - **Deduplication**: Ensuring unique records based on business keys
# MAGIC - **Data Lineage**: Maintaining traceability to source systems