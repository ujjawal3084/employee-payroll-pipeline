"""
Glue Job: S3 Bronze -> S3 Silver (Clean & Dedupe)
---------------------------------------------------
Reads the current state of the Bronze Hudi table (raw employee records)
and writes a cleaned, deduplicated version into the Silver Hudi table.

Why this is a separate job from ingestion:
- Ingestion's only job is "land the raw data exactly as it arrived."
- This job's only job is "make sure Silver only has one clean, correct
  row per employee_id." Keeping them separate means a bug in cleaning
  logic never risks corrupting the raw Bronze copy.

This is a BATCH job (not streaming) -- it reads the latest Bronze
snapshot each time it runs and re-applies cleaning. Hudi's upsert
write to Silver means re-running this job is safe (idempotent):
running it twice with the same Bronze data produces the same Silver
result, not duplicates.
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, trim, upper, row_number
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BRONZE_S3_PATH', 'SILVER_S3_PATH'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

BRONZE_S3_PATH = args['BRONZE_S3_PATH']   # e.g. s3://...-bronze-.../employees/
SILVER_S3_PATH = args['SILVER_S3_PATH']   # e.g. s3://...-silver-.../employees/

# ---- Read the current Bronze Hudi table as a normal (non-streaming) DataFrame ----
bronze_df = spark.read.format("hudi").load(BRONZE_S3_PATH)

# ---- Cleaning rules ----
# 1. Trim stray whitespace from text fields (common real-world data issue)
# 2. Drop rows missing essential fields (a clean row must have a name and a valid salary)
# 3. Standardize department names to a consistent case
cleaned_df = (
    bronze_df
    .withColumn("name", trim(col("name")))
    .withColumn("department", trim(upper(col("department"))))
    .withColumn("designation", trim(col("designation")))
    .filter(col("employee_id").isNotNull())
    .filter(col("name").isNotNull() & (col("name") != ""))
    .filter(col("salary").isNotNull() & (col("salary") > 0))
)

# ---- Deduplication ----
# Even though Bronze already upserts by employee_id, this is a safety net:
# if Bronze ever ends up with more than one row per employee_id (e.g. from a
# replay or a Hudi compaction edge case), keep only the most recent one,
# based on updated_at.
window_spec = Window.partitionBy("employee_id").orderBy(col("updated_at").desc())

deduped_df = (
    cleaned_df
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
)

# ---- Write to Silver as a Hudi upsert ----
hudi_options = {
    'hoodie.table.name': 'employees_silver',
    'hoodie.datasource.write.recordkey.field': 'employee_id',
    'hoodie.datasource.write.precombine.field': 'updated_at',
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
    'hoodie.upsert.shuffle.parallelism': 2,
    'hoodie.insert.shuffle.parallelism': 2,
}

(
    deduped_df.write
    .format("hudi")
    .options(**hudi_options)
    .mode("append")
    .save(SILVER_S3_PATH)
)

job.commit()
