# -*- coding: utf-8 -*-
"""
"DataCo" real-time data pipeline
 Sample Ingested data would be like
 {
    "row_key":"123",
    "click_data" : {
        "user_id": "1",
        "timestamp": "2023-07-07T12:00:00Z",
        "url": "https://www.dataco.com/products/"
    },
    "geo_data": {
        "country": "United States",
        "city": "New York"
    },
    "user_agent_data": {
        "browser": "Chrome",
        "operating_system": "Windows 10",
        "device": "Desktop"
    }
}
"
"""

"""
This sections contains all the necessary modules to be imported
"""
# imports required to run the process in python
import json
import kafka
import pyspark
import mysql.connector
from pyspark.sql.functions import *
from pyspark.sql.types import *

"""
This section contains the Kafka Producer code
Which take the topic name (to identify to which topic the data to be sent)
and json data as an input.
Here, we are assuming that kafka is running on localhost:9092
and client that got created is the Kafka producer, which is a component that can be used to send data to a Kafka topic.
"""

def produce_clickstream_data(topic, data):
    """Produces clickstream data to the specified Kafka topic."""
    client = kafka.KafkaProducer(bootstrap_servers="localhost:9092")
    client.send(topic, json.dumps(data).encode("utf-8"))
    client.flush()

"""
The schema is defined for the JSON data that will be read from Kafka.
"""

# Define the schema for the JSON data
schema = StructType([
    StructField("row_key", StringType(), nullable=False),
    StructField("click_data", StructType([
        StructField("user_id", StringType(), nullable=False),
        StructField("timestamp", TimestampType(), nullable=False),
        StructField("url", StringType(), nullable=False)
    ]), nullable=False),
    StructField("geo_data", StructType([
        StructField("country", StringType(), nullable=False),
        StructField("city", StringType(), nullable=False)
    ]), nullable=False),
    StructField("user_agent_data", StructType([
        StructField("browser", StringType(), nullable=False),
        StructField("operating_system", StringType(), nullable=False),
        StructField("device", StringType(), nullable=False)
    ]), nullable=False)
])

"""
This section contains the Kafka Consumer code along with Data Store and Elasticsearch load.
The below consumer code takes the topic name as an input (to identify from which topic the data has to be read)
As we assumed that kafka is running on localhost:9092 use the same here.
The client that got created is the Kafka consumer object, which is a component that can be used to read the data from the given Kafka topic.
"""

def process_clickstream_data(topic):
    """Processes clickstream data from the specified Kafka topic."""
    client = kafka.KafkaConsumer(topic, bootstrap_servers="localhost:9092")
    for message in client:
        data = json.loads(message.value)
        spark_context = pyspark.SparkContext()
        # Created an RDD from the list of JSON data
        rdd = spark_context.parallelize(data)
        # Created a DataFrame from the RDD and schema
        df = spark_context.createDataFrame(rdd, schema)

        # schema of the MySQL database for the data to be stored
        mysqlSchema = [("user_id", "string"), ("timestamp", "string"), ("url", "string"), ("geo_data", "string"), ("user_agent_data", "string")]

        # Store the data in MySQL database.  Assuming the table name as 'clickstream_data'
        conn = mysql.connector.connect(host="localhost", user="dataco", password="dataco", database="dataco")
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS clickstream_data (%s)", ",".join(mysqlSchema))
        cursor.close()
        conn.close()

        df.write.format("jdbc").option("url", "jdbc:mysql://localhost:5432/dataco").option("user", "dataco").option("password", "dataco").option("dbtable", "clickstream_data").save()

        # aggregating the data by URL and country, and
        # calculating the number of clicks, unique users, and average time spent on each URL by users from each country
        agg_df = df.groupBy("click_data.url", "geo_data.country").agg(
            pyspark.sql.functions.count("*").alias("click_count"),
            pyspark.sql.functions.countDistinct("click_data.userId").alias("unique_users"),
            pyspark.sql.functions.avg("click_data.timestamp").alias("average_time_spent")
        ).withColumnRenamed("click_data.url", "url").withColumnRenamed("geo_data.country", "country")

        # Index the aggregated data in Elasticsearch. Assuming the Index name as 'clickstream'
        # Also assuming that the Elasticsearch index is already created with Mapping defined and loading data incrementally
        agg_df.write.format("org.elasticsearch.spark.sql").option("es.nodes", "localhost:9200").option("es.index.name", "clickstream").save()

"""
Below are sample data and assumed the topic as clickstream-data
"""
if __name__ == "__main__":
    topic = "clickstream-data"
    data = [{
      "row_key":"123",
      "click_data" : {
          "user_id": "1",
          "timestamp": "2023-07-07T12:00:00Z",
          "url": "https://www.dataco.com/products/"
      },
      "geo_data": {
          "country": "United States",
          "city": "New York"
      },
      "user_agent_data": {
          "browser": "Chrome",
          "operating_system": "Windows 10",
          "device": "Desktop"
      }
    }]
    # call to kafka producer
    produce_clickstream_data(topic, data)
    # call to kafka consumer which read the data and store into mysql and aggredate the data and load to Elasticsearch
    process_clickstream_data(topic)