"""
Glue Job: Kafka -> S3 Bronze (Ingestion)
-----------------------------------------
Reads CDC events (Full Load + ongoing changes) from the Kafka topic
'employee-cdc-stream' (produced by AWS DMS) and writes them into the
S3 Bronze layer in Apache Hudi format.

Why Hudi here: DMS produces INSERT/UPDATE/DELETE operations, not just
new rows. Hudi's upsert capability lets us apply those operations
correctly (a row updated in RDS gets updated in Bronze, not duplicated).

This is a Glue STREAMING job (not a batch job) since Kafka is a
continuous source.
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# ---- Job setup ----
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'KAFKA_BOOTSTRAP_SERVERS', 'KAFKA_TOPIC', 'BRONZE_S3_PATH'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

KAFKA_BOOTSTRAP_SERVERS = args['KAFKA_BOOTSTRAP_SERVERS']   # e.g. "52.66.212.137:9092"
KAFKA_TOPIC = args['KAFKA_TOPIC']                            # e.g. "employee-cdc-stream"
BRONZE_S3_PATH = args['BRONZE_S3_PATH']                       # e.g. "s3://ujjawal-payroll-bronze-.../employees/"

# ---- Schema of the DMS CDC JSON message ----
# DMS wraps each row change in {"data": {...}, "metadata": {...}}
data_schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("designation", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("hire_date", StringType(), True),
    StructField("updated_at", StringType(), True),
])

metadata_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("record-type", StringType(), True),
    StructField("operation", StringType(), True),   # load / insert / update / delete
    StructField("schema-name", StringType(), True),
    StructField("table-name", StringType(), True),
])

message_schema = StructType([
    StructField("data", data_schema, True),
    StructField("metadata", metadata_schema, True),
])

# ---- Read from Kafka as a streaming source ----
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

# ---- Parse the JSON value column ----
parsed_df = (
    kafka_df
    .selectExpr("CAST(value AS STRING) as json_value")
    .select(from_json(col("json_value"), message_schema).alias("parsed"))
    .select(
        col("parsed.data.*"),
        col("parsed.metadata.operation").alias("dms_operation"),
        col("parsed.metadata.record-type").alias("dms_record_type"),
    )
    # Skip control messages (create-table events), only keep actual data rows
    .filter(col("dms_record_type") == "data")
    .withColumn("bronze_ingested_at", current_timestamp())
)


def write_to_bronze(batch_df, batch_id):
    """
    For each micro-batch, upsert into the Hudi Bronze table.
    Hudi handles INSERT/UPDATE/DELETE based on the primary key (employee_id),
    so DMS's update/delete operations correctly modify existing Bronze records
    instead of creating duplicates.
    """
    if batch_df.rdd.isEmpty():
        return

    hudi_options = {
        'hoodie.table.name': 'employees_bronze',
        'hoodie.datasource.write.recordkey.field': 'employee_id',
        'hoodie.datasource.write.precombine.field': 'updated_at',
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
        'hoodie.upsert.shuffle.parallelism': 2,
        'hoodie.insert.shuffle.parallelism': 2,
    }

    # DELETE operations: mark rows for removal (Hudi soft/hard delete pattern)
    deletes = batch_df.filter(col("dms_operation") == "delete")
    upserts = batch_df.filter(col("dms_operation") != "delete")

    if upserts.rdd.isEmpty() is False:
        (
            upserts.write
            .format("hudi")
            .options(**hudi_options)
            .mode("append")
            .save(BRONZE_S3_PATH)
        )

    if deletes.rdd.isEmpty() is False:
        delete_options = dict(hudi_options)
        delete_options['hoodie.datasource.write.operation'] = 'delete'
        (
            deletes.write
            .format("hudi")
            .options(**delete_options)
            .mode("append")
            .save(BRONZE_S3_PATH)
        )


# ---- Start the streaming query ----
query = (
    parsed_df.writeStream
    .foreachBatch(write_to_bronze)
    .option("checkpointLocation", BRONZE_S3_PATH + "_checkpoints/")
    .trigger(processingTime="60 seconds")  # micro-batch every 60s, good fit for payroll-scale data
    .start()
)

query.awaitTermination()

job.commit()
