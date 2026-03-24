import sys
import os
import time
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofweek, hour, count, avg, desc, unix_timestamp, when


SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:920978932753:accidents-pipeline-alerts"
SAGEMAKER_ROLE_ARN = "arn:aws:iam::920978932753:role/service-role/AmazonSageMaker-ExecutionRole-20251211T173238"
AWS_REGION = "us-east-1"
S3_BUCKET_NAME = "us-accidents"
S3_INPUT_PATH = f"s3a://{S3_BUCKET_NAME}/raw_data/US_Accidents.csv"
S3_OUTPUT_PATH = f"s3a://{S3_BUCKET_NAME}/processed_data/"
SAGEMAKER_INPUT_DATA = f"s3://{S3_BUCKET_NAME}/processed_data/ml_ready/"

def create_spark_session():
    """
    Initializes Spark with Hadoop-AWS dependencies and injects credentials 
    from environment variables.
    """
    print("Initializing Spark")
    
    # Create Spark Session with AWS configurations
    spark = SparkSession.builder \
        .appName("US_Accidents_Data_Pipeline") \
        .master("local[1]") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.network.timeout", "600s") \
        .config("spark.executor.heartbeatInterval", "120s") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60") \
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400") \
        .config("spark.hadoop.fs.s3a.change.detection.source.ttl", "30") \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "10000") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")
    print("Spark Session created.")
    return spark

def ingest_data(spark, path):
    print(f"\nPulling Data from S3: {path}")
    try:
        # Read CSV data from S3
        df = spark.read.csv(path, header=True, inferSchema=True)
        return df
    except Exception as e:
        print(f"Could not read from S3. {e}")
        sys.exit(1)

def inspect_data(df):
    print("\nVerifying Data")
    # Show basic info about the DataFrame
    print(f"Row Count: {df.count()}")
    print("Schema:")
    df.printSchema()
    print("Preview:")
    df.show(5)

def clean_data(df):
    print("\nCleaning & Standardizing Data")
    
    # Removing irrelevant columns
    cols_to_drop = ["ID", "Source", "End_Lat", "End_Lng", "Description", "Street", "Zipcode",
        "Country", "Timezone", "Airport_Code", "Weather_Timestamp", "Wind_Chill(F)",
        "Civil_Twilight", "Nautical_Twilight", "Astronomical_Twilight"]
    
    print(f"Dropping columns: {cols_to_drop}")
    df_clean = df.drop(*cols_to_drop)
    
    # Standardize Column Names
    df_clean = df_clean \
        .withColumnRenamed("Severity", "severity") \
        .withColumnRenamed("Start_Time", "start_time") \
        .withColumnRenamed("End_Time", "end_time") \
        .withColumnRenamed("Start_Lat", "start_lat") \
        .withColumnRenamed("Start_Lng", "start_lng") \
        .withColumnRenamed("Distance(mi)", "distance_mi") \
        .withColumnRenamed("City", "city") \
        .withColumnRenamed("County", "county") \
        .withColumnRenamed("State", "state") \
        .withColumnRenamed("Temperature(F)", "temperature_f") \
        .withColumnRenamed("Humidity(%)", "humidity_perc") \
        .withColumnRenamed("Pressure(in)", "pressure_in") \
        .withColumnRenamed("Visibility(mi)", "visibility_mi") \
        .withColumnRenamed("Wind_Direction", "wind_direction") \
        .withColumnRenamed("Wind_Speed(mph)", "wind_speed_mph") \
        .withColumnRenamed("Precipitation(in)", "precipitation_in") \
        .withColumnRenamed("Weather_Condition", "weather_condition") \
        .withColumnRenamed("Amenity", "amenity") \
        .withColumnRenamed("Bump", "bump") \
        .withColumnRenamed("Crossing", "crossing") \
        .withColumnRenamed("Give_Way", "give_way") \
        .withColumnRenamed("Junction", "junction") \
        .withColumnRenamed("No_Exit", "no_exit") \
        .withColumnRenamed("Railway", "railway") \
        .withColumnRenamed("Roundabout", "roundabout") \
        .withColumnRenamed("Station", "station") \
        .withColumnRenamed("Stop", "stop") \
        .withColumnRenamed("Traffic_Calming", "traffic_calming") \
        .withColumnRenamed("Traffic_Signal", "traffic_signal") \
        .withColumnRenamed("Turning_Loop", "turning_loop") \
        .withColumnRenamed("Sunrise_Sunset", "sunrise_sunset")

    # Drop Duplicates
    initial_count = df_clean.count()
    df_clean = df_clean.dropDuplicates()
    print(f"Removed {initial_count - df_clean.count()} duplicate rows.")
    
    # Handle Nulls
    df_clean = df_clean.dropna(subset=['start_time', 'end_time', 'start_lat', 'start_lng'])
    
    # Fill Nulls (Weather Columns)
    df_clean = df_clean.fillna({
        'temperature_f': 0.0,
        'humidity_perc': 0.0,
        'pressure_in': 0.0,
        'visibility_mi': 0.0,
        'wind_speed_mph': 0.0,
        'precipitation_in': 0.0,
        'weather_condition': 'Unknown'
    })
    
    print(f"Cleaned Row Count: {df_clean.count()}")
    return df_clean

def transform_data(df):
    print("\nTransforming Data")
    
    df.cache()

    # Extract Time Features
    df_trans = df.withColumn("year", year(col("start_time"))) \
                 .withColumn("month", month(col("start_time"))) \
                 .withColumn("hour", hour(col("start_time"))) \
                 .withColumn("day_of_week", dayofweek(col("start_time")))
    
    # Calculate Accident Duration (in Minutes)
    df_trans = df_trans.withColumn("duration_min", 
        (unix_timestamp(col("end_time")) - unix_timestamp(col("start_time"))) / 60
    )

    # Rush Hour defined as: 7-9 AM and 4-6 PM
    df_trans = df_trans.withColumn("is_rush_hour", 
        when(col("hour").isin(7, 8, 9, 16, 17, 18), 1).otherwise(0)
    )
    
    # Day 1=Sunday, 7=Saturday
    df_trans = df_trans.withColumn("is_weekend", 
        when(col("day_of_week").isin(1, 7), 1).otherwise(0)
    )

    print("Transformation Complete.")
    return df_trans

def aggregate_data(df):
    print("\nAggregating Insights")
    
    # 1. State Hotspots
    print("\n1. State Hotspots:")
    df1 = df.groupBy("state").count().orderBy(desc("count"))
    df1.show(10)
    
    # 2. Rush Hour Analysis
    print("\n2. Rush Hour Impact:")
    df2 = df.groupBy("is_rush_hour") \
      .agg(
          count("*").alias("total_accidents"),
          avg("severity").alias("avg_severity")
      )
    df2.show()
    
    # 3. Weather Conditions
    print("\n3. Weather Impact:")
    df3 = df.groupBy("weather_condition") \
      .agg(
          avg("severity").alias("avg_severity"), 
          count("*").alias("total_accidents")
      ) \
      .filter(col("total_accidents") > 20) \
      .orderBy(desc("avg_severity"))
    df3.show(10)
      
    # 4. Traffic Objects (Signals/Junctions)
    # We compare accidents occurring at a Traffic Signal vs. Open Road.
    print("\n4. Traffic Signals vs. Open Road:")
    df4 = df.groupBy("traffic_signal") \
      .agg(
          count("*").alias("accident_count"),
          avg("severity").alias("avg_severity"),
          avg("duration_min").alias("avg_clearance_time")
      ) 
    df4.show()

    # 5. Top Cities by Clearing Time (Duration)
    print("\n5. Emergency Response Efficiency - Slowest Cities to Clear:")
    df5 = df.groupBy("city", "state") \
      .agg(
          avg("duration_min").alias("avg_duration_min"), 
          count("*").alias("total_accidents")
      ) \
      .filter(col("total_accidents") > 10) \
      .orderBy(desc("avg_duration_min"))
    df5.show(5)

    return df1, df2, df3, df4, df5

def run_spark_sql_analysis(spark, df):
    print("\nSpark SQL Analysis")
    
    # Register DataFrame as a temporary SQL view
    df.createOrReplaceTempView("us_accidents")
        
    # Query 1: Top 10 Cities with Highest Accidents
    print("\n1. Top 10 Cities by Accident Count:")
    spark.sql("""
        SELECT city, state, COUNT(*) as accident_count
        FROM us_accidents
        GROUP BY city, state
        ORDER BY accident_count DESC
        LIMIT 10
    """).show()

    # Query 2: Which day of week has most accidents?
    print("\n2. Accident Count by Day of Week:")
    spark.sql("""
        SELECT 
            CASE 
                WHEN day_of_week = 1 THEN 'Sunday'
                WHEN day_of_week = 2 THEN 'Monday'
                WHEN day_of_week = 3 THEN 'Tuesday'
                WHEN day_of_week = 4 THEN 'Wednesday'
                WHEN day_of_week = 5 THEN 'Thursday'
                WHEN day_of_week = 6 THEN 'Friday'
                WHEN day_of_week = 7 THEN 'Saturday'
            END as day_name,
            COUNT(*) as total_accidents
        FROM us_accidents
        GROUP BY day_of_week
        ORDER BY total_accidents DESC
    """).show()

    # Query 3: Most Dangerous States
    print("\n3. States with Highest Average Severity:")
    spark.sql("""
        SELECT state, AVG(severity) as avg_severity, COUNT(*) as total
        FROM us_accidents
        GROUP BY state
        HAVING total > 100
        ORDER BY avg_severity DESC
        LIMIT 5
    """).show()

    # Query 4: Visibility vs Severity
    print("\n4. Average Severity by Visibility Range:")
    spark.sql("""
        SELECT 
            CASE 
                WHEN visibility_mi < 1 THEN 'Very Low (<1mi)'
                WHEN visibility_mi BETWEEN 1 AND 5 THEN 'Low (1-5mi)'
                ELSE 'Normal (>5mi)'
            END as visibility_category,
            AVG(severity) as avg_severity,
            COUNT(*) as count
        FROM us_accidents
        GROUP BY 1
        ORDER BY avg_severity DESC
    """).show()

    # Query 5: Monthly Accident Trend
    print("\n5. Monthly Accident Trend:")
    spark.sql("""
        SELECT month, COUNT(*) as total_accidents
        FROM us_accidents
        GROUP BY month
        ORDER BY month ASC
    """).show()
    
def save_to_s3(df, df1, df2, df3, df4, df5):
    print(f"\nSaving Processed Data to S3: {S3_OUTPUT_PATH}")
    
    try:
        ml_df = df.drop("roundabout", "turning_loop", "duration_min", "start_time", "end_time", "city", "county", "year")

        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(S3_OUTPUT_PATH + "master_clean")
        ml_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(S3_OUTPUT_PATH + "ml_ready")
        df1.coalesce(1).write.mode("overwrite").option("header", "true").csv(os.path.join(S3_OUTPUT_PATH, "state_hotspots"))
        df2.coalesce(1).write.mode("overwrite").option("header", "true").csv(os.path.join(S3_OUTPUT_PATH, "rush_hour_impact"))
        df3.coalesce(1).write.mode("overwrite").option("header", "true").csv(os.path.join(S3_OUTPUT_PATH, "weather_impact"))
        df4.coalesce(1).write.mode("overwrite").option("header", "true").csv(os.path.join(S3_OUTPUT_PATH, "traffic_signals_analysis"))
        df5.coalesce(1).write.mode("overwrite").option("header", "true").csv(os.path.join(S3_OUTPUT_PATH, "slowest_cities_to_clear"))
        print("Data successfully written to S3 via Spark Write.")
        
    except Exception as e:
        print(f"Failed to write to S3. {e}")
        sys.exit(1)

def trigger_automl_job():
    """Triggers a SageMaker Autopilot job for the processed data."""
    print("\nTriggering SageMaker Autopilot")
    sm_client = boto3.client("sagemaker", region_name=AWS_REGION)
    
    # Generate job name using timestamp
    timestamp = time.strftime("%m-%d-%H-%M")
    auto_ml_job_name = f"automl-job-{timestamp}"
    
    print(f"Launching Job: {auto_ml_job_name}...")

    try:
        sm_client.create_auto_ml_job(
            AutoMLJobName=auto_ml_job_name,
            InputDataConfig=[{
                'DataSource': {
                    'S3DataSource': {
                        'S3DataType': 'S3Prefix',
                        'S3Uri': SAGEMAKER_INPUT_DATA
                    }
                },
                'TargetAttributeName': 'severity'
            }],
            OutputDataConfig={
                'S3OutputPath': f"s3://{S3_BUCKET_NAME}/models/"
            },
            ProblemType='MulticlassClassification',
            AutoMLJobObjective={'MetricName': 'Accuracy'},
            AutoMLJobConfig={
                'CompletionCriteria': {
                    'MaxRuntimePerTrainingJobInSeconds': 600, # Limit each model to 10 minutes
                    'MaxAutoMLJobRuntimeInSeconds': 3600 # Limit total job to 1 hour
                }
            },
            RoleArn=SAGEMAKER_ROLE_ARN
        )
        print(f"SUCCESS: Autopilot Job '{auto_ml_job_name}' Started.")
        return auto_ml_job_name
    except Exception as e:
        print(f"Failed to trigger SageMaker: {e}")
        raise e

def send_sns_notification(status, message):
    print(f"\nSending SNS Notification: {status}")
    try:
        sns = boto3.client("sns", region_name=AWS_REGION)
        subject = f"Pipeline {status}: US Accidents Data"
        
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=message,
            Subject=subject
        )
        print("Notification Sent.")
    except Exception as e:
        print(f"Error sending SNS: {e}")

if __name__ == "__main__":
    try:
        spark = create_spark_session()

        # Task 1: Direct Ingestion from S3
        raw_df = ingest_data(spark, S3_INPUT_PATH)
        inspect_data(raw_df)

        # Task 2: Data Processing with PySpark
        clean_df = clean_data(raw_df)
        df = transform_data(clean_df)
        inspect_data(df)
        df1, df2, df3, df4, df5 = aggregate_data(df)

        # Task 3: Store Processed Data Back to S3
        save_to_s3(df, df1, df2, df3, df4, df5)

        # Task 4: Data Analysis Using Spark SQL
        run_spark_sql_analysis(spark, df)

        spark.stop()

        # Task 4: Machine Learning with AWS SageMaker Autopilot
        job_name = trigger_automl_job()
        
        # Send SUCCESS Notification
        success_msg = (f"US Accidents Pipeline COMPLETED SUCCESSFULLY.\n"
                       f"1. Data Cleaned & Saved to {S3_OUTPUT_PATH}\n"
                       f"2. SageMaker Autopilot Job Started: {job_name}")
        
        send_sns_notification("SUCCESS", success_msg)
        
    except Exception as e:
        # Failure Notification
        error_msg = f"Pipeline CRASHED.\nError Details: {str(e)}"
        print(error_msg)
        send_sns_notification("FAILED", error_msg)
        sys.exit(1)