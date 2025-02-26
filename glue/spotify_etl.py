import sys
import logging
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, TimestampType

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Glue and Spark Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Define the S3 paths for your data files
songs_path = "s3://spotify-streaming-data/songs.csv"
streams1_path = "s3://spotify-streaming-data/streams1.csv"
streams2_path = "s3://spotify-streaming-data/streams2.csv"
streams3_path = "s3://spotify-streaming-data/streams3.csv"

# Log information about the start of the ETL job
logger.info("Starting ETL job to calculate average listening duration per genre per day")

# Load the songs data
logger.info(f"Loading songs data from {songs_path}")
songs_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [songs_path]},
    format="csv",
    format_options={"withHeader": True, "separator": ","}
).toDF()

# Load all streams data and union them
logger.info("Loading and combining streams data")
streams1_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [streams1_path]},
    format="csv",
    format_options={"withHeader": True, "separator": ","}
).toDF()

streams2_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [streams2_path]},
    format="csv",
    format_options={"withHeader": True, "separator": ","}
).toDF()

streams3_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [streams3_path]},
    format="csv",
    format_options={"withHeader": True, "separator": ","}
).toDF()

# Combine all streams data
streams_df = streams1_df.union(streams2_df).union(streams3_df)

# Extract date from listen_time
logger.info("Processing data for KPI calculation")
streams_df = streams_df.withColumn(
    "listen_date", 
    F.to_date("listen_time")
)

# Join streams with songs to get genre information
logger.info("Joining streams data with songs data")
streams_with_songs = streams_df.join(
    songs_df.select("track_id", "track_genre", "duration_ms"),
    on="track_id",
    how="inner"
)

# Calculate the average listening duration per genre per day
logger.info("Calculating average listening duration per genre per day")
kpi_result = streams_with_songs.groupBy(
    "track_genre", 
    "listen_date"
).agg(
    F.avg("duration_ms").alias("avg_duration_ms"),
    F.count("*").alias("stream_count")
)

# Convert milliseconds to minutes for better readability
kpi_result = kpi_result.withColumn(
    "avg_duration_minutes", 
    F.round(F.col("avg_duration_ms") / 60000, 2)
).orderBy("listen_date", "track_genre")

# Display the results
logger.info("Average Listening Duration per Genre per Day:")
kpi_result.show(20, False)

kpi_result = kpi_result.coalesce(1)
# Convert back to DynamicFrame for writing
kpi_dynamic_frame = DynamicFrame.fromDF(kpi_result, glueContext, "kpi_dynamic_frame")

# Write the KPI results to S3
s3_output_path = "s3://spotify-streaming-data/kpi_genre_duration/"
logger.info(f"Writing KPI results to {s3_output_path}")


glueContext.write_dynamic_frame.from_options(
    frame=kpi_dynamic_frame,
    connection_type="s3",
    connection_options={"path": s3_output_path},
    format="csv"
)

# Log the successful completion of the job
logger.info("KPI Calculation Job Completed Successfully")

