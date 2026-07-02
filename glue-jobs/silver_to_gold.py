"""
Glue Job: S3 Silver -> S3 Gold (Aggregate)
--------------------------------------------
Reads the cleaned Silver Hudi table and produces department-level
payroll summaries into the Gold layer, in plain Parquet format.

Why Parquet (not Hudi) here: Gold is built fresh each run from Silver --
there's no need to upsert individual rows, since the whole aggregate
table is simply recomputed and overwritten. Plain Parquet is faster to
write and read for this "write once, read many times for reporting"
access pattern, and avoids the overhead of Hudi's update-tracking
machinery, which only pays off when you have frequent row-level upserts.
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, count, avg, sum as spark_sum, round as spark_round

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SILVER_S3_PATH', 'GOLD_S3_PATH'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

SILVER_S3_PATH = args['SILVER_S3_PATH']   # e.g. s3://...-silver-.../employees/
GOLD_S3_PATH = args['GOLD_S3_PATH']       # e.g. s3://...-gold-.../department_payroll_summary/

# ---- Read the cleaned Silver Hudi table ----
silver_df = spark.read.format("hudi").load(SILVER_S3_PATH)

# ---- Aggregate: department-level payroll summary ----
# This is the kind of table a payroll dashboard would actually query:
# headcount, average pay, and total payroll cost per department.
department_summary_df = (
    silver_df
    .groupBy("department")
    .agg(
        count("employee_id").alias("headcount"),
        spark_round(avg("salary"), 2).alias("avg_salary"),
        spark_round(spark_sum("salary"), 2).alias("total_payroll_cost"),
    )
    .orderBy(col("department"))
)

# ---- Write to Gold as plain Parquet, overwriting each run ----
# "overwrite" mode is correct here because Gold should always reflect
# a fresh recomputation from the latest Silver data -- not an
# accumulation of every past run's numbers.
(
    department_summary_df.write
    .format("parquet")
    .mode("overwrite")
    .save(GOLD_S3_PATH)
)

job.commit()
