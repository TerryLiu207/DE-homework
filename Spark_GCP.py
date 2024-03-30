import PySpark
from PySpark.sql import SparkSession
from PySpark.conf import SparkConf
from PySpark.context import SparkContext
from PySpark.sql import SparkSession

# Path to your Google Cloud service account key file
credentials_location = 'C:\\Users\\13612\\Datacamp\\Project\\mbta-weather-e95649677660.json'

# Path to the GCS connector JAR file
gcs_connector_path = 'C:\\Users\\13612\\Datacamp\\Project\\gcs-connector-hadoop3-2.2.5.jar'

# Path to the BigQuery connector JAR file
bigquery_connector_path = 'C:\\Users\\13612\\Datacamp\\setup\\spark-3.5-bigquery-0.36.1.jar'

# Creating SparkSession with both GCS and BigQuery configurations
spark = SparkSession.builder \
    .appName('example-app') \
    .config('spark.jars', ','.join([gcs_connector_path, bigquery_connector_path])) \
    .config('spark.hadoop.google.cloud.auth.service.account.enable', 'true') \
    .config('spark.hadoop.google.cloud.auth.service.account.json.keyfile', credentials_location) \
    .config('spark.hadoop.fs.AbstractFileSystem.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS') \
    .config('spark.hadoop.fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem') \
    .config('spark.hadoop.fs.gs.auth.service.account.json.keyfile', credentials_location) \
    .config('spark.hadoop.fs.gs.auth.service.account.enable', 'true') \
    .config('spark.hadoop.fs.gs.project.id', 'mbta-weather') \
    .getOrCreate()


from google.cloud import storage
import pandas as pd

# Initialize GCS client
client = storage.Client()

# Define the bucket name
bucket_name = "mage_weather"

# List all blobs (files) in the bucket
blobs = client.list_blobs(bucket_name)

# Create lists to store file names and last modified timestamps
file_names = []
last_modified = []

# Extract file names and last modified timestamps from blobs
for blob in blobs:
    file_names.append(blob.name)
    last_modified.append(blob.updated)

# Create a DataFrame from the lists
df = pd.DataFrame({"File Name": file_names, "Last Modified": last_modified})
df1=df.sort_values(by="Last Modified", ascending=False)
most_recent_file_name = df1.iloc[0]["File Name"]

df2 = spark.read.option("header", "true").csv(f'gs://mage_weather/{most_recent_file_name}')
df2.registerTempTable('weather_data')

df_result = spark.sql("""
SELECT validTime,
    CASE 
        WHEN SRP <= 0.16 THEN 0
        WHEN SRP <= 0.4 THEN 1
        WHEN SRP <= 0.5 THEN 2
        WHEN SRP <= 0.65 THEN 3
        WHEN SRP <= 0.8 THEN 4
        WHEN SRP <= 1 THEN 5
    END AS SRP_result,

    CASE 
        WHEN FI = 0 THEN 0
        WHEN FI <= 0.2 THEN 1
        WHEN FI <= 0.5 THEN 2
        WHEN FI <= 1 THEN 3
        WHEN FI <= 1.5 THEN 4
        WHEN FI > 1.5 THEN 5
    END AS FI_result,

    CASE 
        WHEN SP = 0 THEN 0
        WHEN SP <= 3 THEN 1
        WHEN SP <= 5 THEN 2
        WHEN SP <= 7 THEN 3
        WHEN SP <= 16 THEN 4
        WHEN SP > 16 THEN 5
    END AS SP_result,

    CASE 
        WHEN SW = 0 THEN 0
        WHEN SW <= 2 THEN 1
        WHEN SW <= 5 THEN 2
        WHEN SW <= 10 THEN 3
        WHEN SW <= 15 THEN 4
        WHEN SW > 15 THEN 5
    END AS SW_result,

    CASE 
        WHEN WindGust * 0.621 <= 6 THEN 0
        WHEN WindGust * 0.621 <= 12 THEN 1
        WHEN WindGust * 0.621 <= 21 THEN 2
        WHEN WindGust * 0.621 <= 46 THEN 3
        WHEN WindGust * 0.621 <= 63 THEN 4
        WHEN WindGust * 0.621 > 63 THEN 5
    END AS WindGust_result,

    ROUND(0.033 + (INT(SRP_result) * 0.2731) + (INT(FI_result) * 0.1594) + (INT(SP_result) * 0.2973) + (INT(SW_result) * 0.1415) + (INT(WindGust_result) * 0.1923), 0) AS final_result
FROM weather_data
""")
# Path to save CSV files in the same folder on GCS
output_gcs_path_result = "gs://weather_results/df_result.csv"
output_gcs_path_df2 = "gs://weather_results/df2.csv"

# Write DataFrame df_result to CSV format on GCS
df_result.coalesce(1).write.csv(output_gcs_path_result, mode='overwrite', header=True)

# Write DataFrame df2 to CSV format on GCS
df2.coalesce(1).write.csv(output_gcs_path_df2, mode='overwrite', header=True)

input('Press enter to exit......')
