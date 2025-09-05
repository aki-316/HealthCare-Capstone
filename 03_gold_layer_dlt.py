# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Analytics and Business Intelligence
# MAGIC
# MAGIC This notebook creates analytics-ready datasets with advanced business logic, fraud detection, and aggregated reporting using Delta Live Tables.

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fraud Detection Functions

# COMMAND ----------

def add_fraud_score(df):
    """
    Add comprehensive fraud risk scoring to claims data
    Uses multiple factors: amount anomalies, timing issues, provider reputation
    """
    return (df
        # Amount anomaly scoring (30% weight)
        .withColumn("amount_deviation",
                    when(col("avg_claim_amount") > 0,
                         abs(col("claim_amount") - col("avg_claim_amount")) / col("avg_claim_amount"))
                    .otherwise(lit(0)))
        .withColumn("amount_score",
                    when(col("amount_deviation") > 2.0, 30)  # >200% deviation
                    .when(col("amount_deviation") > 1.0, 15) # >100% deviation
                    .otherwise(0))

        # Timing anomaly scoring (25% weight)  
        .withColumn("days_diff", datediff(col("claim_date"), col("service_date")))
        .withColumn("timing_score",
                    when(col("days_diff") < 0, 25)       # Future service date - major red flag
                    .when(col("days_diff") > 365, 20)    # Very old claim
                    .when(col("days_diff") > 90, 10)     # Old claim
                    .otherwise(0))

        # Provider reputation scoring (25% weight)
        .withColumn("provider_score",
                    when(col("provider_reputation_score") < 0.5, 25)  # Low reputation
                    .when(col("provider_reputation_score") < 0.7, 15) # Medium reputation
                    .otherwise(0))

        # Volume anomaly scoring (20% weight)
        .withColumn("volume_score",
                    when(col("member_claim_frequency") > 10, 20)  # High frequency claims
                    .when(col("member_claim_frequency") > 5, 10)
                    .otherwise(0))

        # Calculate total fraud risk score (capped at 100)
        .withColumn("fraud_risk_score",
                    least(col("amount_score") + col("timing_score") + 
                          col("provider_score") + col("volume_score"),
                          lit(100.0)))

        # Assign risk categories
        .withColumn("risk_category",
                    when(col("fraud_risk_score") >= 70, "HIGH")
                    .when(col("fraud_risk_score") >= 40, "MEDIUM")
                    .otherwise("LOW"))
        
        # Add investigation priority
        .withColumn("investigation_priority",
                    when((col("fraud_risk_score") >= 70) & (col("claim_amount") > 10000), "URGENT")
                    .when(col("fraud_risk_score") >= 70, "HIGH") 
                    .when(col("fraud_risk_score") >= 40, "MEDIUM")
                    .otherwise("LOW"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Primary Analytics Table - Enriched Claims

# COMMAND ----------

@dlt.table(
    name="claims_analytics",
    comment="Comprehensive claims analytics with fraud detection and member/provider enrichment",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_claims_analytics():
    """
    Create the primary analytics table with comprehensive enrichment:
    - Member demographic data
    - Provider information and reputation scoring  
    - Fraud risk assessment
    - Business metrics and KPIs
    """
    # Read from Silver layer
    claims = spark.table("capstone.silver.claims_processed")
    members = spark.table("capstone.silver.members_processed")
    providers = spark.table("capstone.silver.providers_processed")
    
    # Calculate provider statistics and reputation
    provider_stats = (claims
        .groupBy("provider_id")
        .agg(
            avg("claim_amount").alias("avg_claim_amount"),
            count("claim_id").alias("total_claims"),
            countDistinct("member_id").alias("unique_members"),
            max("claim_date").alias("last_claim_date"),
            sum("claim_amount").alias("total_claim_value"),
            avg("days_to_claim").alias("avg_processing_days")
        )
    )
    
    # Calculate member claim frequency
    member_stats = (claims
        .groupBy("member_id")
        .agg(
            count("claim_id").alias("member_claim_frequency"),
            sum("claim_amount").alias("member_total_claims"),
            max("claim_date").alias("member_last_claim")
        )
    )
    
    # Get distinct provider info (one record per provider)
    provider_summary = (providers
        .groupBy("provider_id", "provider_name")
        .agg(
            first("is_active").alias("provider_is_active"),
            count("specialty").alias("specialty_count"),
            count("address").alias("location_count"),
            max("last_verified").alias("last_verified")
        )
    )
    
    # Create enriched claims dataset
    enriched_claims = (claims
        # Join with member data
        .join(members.select("MemberID", "age", "is_active_member", "Gender", 
                           "Region", "PlanType", "has_complete_profile"), 
              claims.member_id == col("MemberID"), "left")
        .drop("MemberID")
        
        # Join with provider summary
        .join(provider_summary, "provider_id", "left")
        
        # Join with provider statistics
        .join(provider_stats, "provider_id", "left")
        
        # Join with member statistics  
        .join(member_stats, "member_id", "left")
        
        # Calculate provider reputation score
        .withColumn("provider_reputation_score",
                    when(col("total_claims") > 100, 0.9)      # High volume, established
                    .when(col("total_claims") > 50, 0.7)      # Medium volume
                    .when(col("total_claims") > 10, 0.5)      # Low volume
                    .otherwise(0.3))                          # Very new/low volume
        
        # Add business metrics
        .withColumn("claim_efficiency_score",
                    when(col("avg_processing_days") <= 1, 100)
                    .when(col("avg_processing_days") <= 7, 80)
                    .when(col("avg_processing_days") <= 30, 60)
                    .otherwise(40))
    )
    
    # Apply fraud scoring
    return add_fraud_score(enriched_claims)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monthly Claims Summary

# COMMAND ----------

@dlt.table(
    name="claims_summary",
    comment="Monthly aggregated claims summary by provider and risk category",
    table_properties={
        "quality": "gold"
    }
)
def gold_monthly_claims_summary():
    """
    Create monthly aggregated reporting for business intelligence
    """
    claims_analytics = spark.table("capstone.gold.claims_analytics")
    
    return (claims_analytics
        .withColumn("claim_month", date_format(col("service_date"), "yyyy-MM"))
        .withColumn("claim_year", year(col("service_date")))
        .withColumn("claim_quarter", concat(col("claim_year"), lit("-Q"), quarter(col("service_date"))))
        
        .groupBy("claim_month", "claim_year", "claim_quarter", "provider_name", "risk_category")
        .agg(
            sum("claim_amount").alias("total_amount"),
            count("claim_id").alias("claim_count"),
            avg("fraud_risk_score").alias("avg_fraud_score"),
            countDistinct("member_id").alias("unique_members"),
            sum(when(col("investigation_priority") == "URGENT", 1).otherwise(0)).alias("urgent_cases"),
            sum(when(col("investigation_priority") == "HIGH", 1).otherwise(0)).alias("high_priority_cases"),
            avg("claim_amount").alias("avg_claim_amount"),
            max("claim_amount").alias("max_claim_amount"),
            sum(when(col("is_high_value_claim"), 1).otherwise(0)).alias("high_value_claim_count")
        )
        .orderBy("claim_month", "provider_name", "risk_category")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Provider Performance

# COMMAND ----------

@dlt.table(
    name="provider_performance",
    comment="Provider performance metrics and rankings",
    table_properties={
        "quality": "gold"
    }
)
def gold_provider_performance():
    """
    Create provider performance metrics for operational dashboards
    """
    claims_analytics = spark.table("capstone.gold.claims_analytics")
    
    # Calculate provider-level metrics
    provider_metrics = (claims_analytics
        .groupBy("provider_id", "provider_name")
        .agg(
            count("claim_id").alias("total_claims"),
            sum("claim_amount").alias("total_claim_value"),
            avg("claim_amount").alias("avg_claim_amount"),
            countDistinct("member_id").alias("unique_members_served"),
            avg("fraud_risk_score").alias("avg_fraud_risk"),
            sum(when(col("risk_category") == "HIGH", 1).otherwise(0)).alias("high_risk_claims"),
            sum(when(col("risk_category") == "MEDIUM", 1).otherwise(0)).alias("medium_risk_claims"),
            sum(when(col("risk_category") == "LOW", 1).otherwise(0)).alias("low_risk_claims"),
            avg("days_to_claim").alias("avg_claim_processing_days"),
            first("provider_reputation_score").alias("reputation_score"),
            first("specialty_count").alias("specialty_count"),
            first("location_count").alias("location_count"),
            max("service_date").alias("last_service_date"),
            min("service_date").alias("first_service_date")
        )
    )
    
    # Add performance rankings using window functions
    window_total_value = Window.orderBy(desc("total_claim_value"))
    window_avg_fraud = Window.orderBy("avg_fraud_risk")
    window_claim_count = Window.orderBy(desc("total_claims"))
    
    return (provider_metrics
        .withColumn("value_rank", row_number().over(window_total_value))
        .withColumn("fraud_risk_rank", row_number().over(window_avg_fraud))
        .withColumn("volume_rank", row_number().over(window_claim_count))
        
        # Calculate risk percentage
        .withColumn("high_risk_percentage", 
                    round((col("high_risk_claims") / col("total_claims")) * 100, 2))
        
        # Provider efficiency score
        .withColumn("efficiency_score",
                    when(col("avg_claim_processing_days") <= 1, 100)
                    .when(col("avg_claim_processing_days") <= 3, 90)
                    .when(col("avg_claim_processing_days") <= 7, 75)
                    .when(col("avg_claim_processing_days") <= 14, 60)
                    .otherwise(40))
        
        # Overall provider score (combination of factors)
        .withColumn("overall_provider_score",
                    round(
                        (col("reputation_score") * 40) +
                        (col("efficiency_score") * 30) + 
                        ((100 - col("high_risk_percentage")) * 30), 2
                    ))
        
        .withColumn("calculated_timestamp", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Member risk profile

# COMMAND ----------

@dlt.table(
    name="member_risk_profile",
    comment="Member-level risk assessment and claim patterns",
    table_properties={
        "quality": "gold"
    }
)
def gold_member_risk_profile():
    """
    Create member risk profiles for personalized care management
    """
    claims_analytics = spark.table("capstone.gold.claims_analytics")
    
    return (claims_analytics
        .groupBy("member_id")
        .agg(
            first("age").alias("member_age"),
            first("Gender").alias("member_gender"),
            first("Region").alias("member_region"),
            first("PlanType").alias("plan_type"),
            first("is_active_member").alias("is_active"),
            
            # Claim patterns
            count("claim_id").alias("total_claims"),
            sum("claim_amount").alias("total_claim_amount"),
            avg("claim_amount").alias("avg_claim_amount"),
            max("claim_amount").alias("max_claim_amount"),
            countDistinct("provider_id").alias("unique_providers"),
            
            # Risk indicators
            avg("fraud_risk_score").alias("avg_fraud_risk"),
            sum(when(col("risk_category") == "HIGH", 1).otherwise(0)).alias("high_risk_claims"),
            sum(when(col("is_high_value_claim"), 1).otherwise(0)).alias("high_value_claims"),
            
            # Medical complexity
            avg(size(col("icd10_codes_array"))).alias("avg_diagnoses_per_claim"),
            avg(size(col("cpt_codes_array"))).alias("avg_procedures_per_claim"),
            
            # Timing patterns
            avg("days_to_claim").alias("avg_claim_submission_days"),
            max("service_date").alias("last_service_date"),
            min("service_date").alias("first_service_date")
        )
        
        # Calculate member risk tier
        .withColumn("member_risk_tier",
                    when(col("avg_fraud_risk") >= 70, "HIGH_RISK")
                    .when(col("avg_fraud_risk") >= 40, "MEDIUM_RISK") 
                    .when(col("total_claims") > 20, "HIGH_UTILIZATION")
                    .otherwise("LOW_RISK"))
        
        # Care management flags
        .withColumn("needs_case_management",
                    when((col("total_claim_amount") > 50000) | 
                         (col("high_risk_claims") > 0) |
                         (col("total_claims") > 15), True)
                    .otherwise(False))
        
        .withColumn("calculated_timestamp", current_timestamp())
    )