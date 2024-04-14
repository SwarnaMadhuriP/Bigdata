#importing the necessary python packages.

from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, udf, current_timestamp, date_trunc, regexp_replace, trim, col, explode, split, desc
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql.functions import lit
from pyspark import SparkConf

import sys
import os
import shutil
import spacy
import logging

import nltk
from nltk.corpus import stopwords


nltk.download('averaged_perceptron_tagger')
nltk.download('stopwords')

# Checking for sufficient arguments
if len(sys.argv) != 5:
    print("Please provide sufficient arguments.")
    print('Usage: spark_connector.py <CHECKPOINT_DIR> <BOOTSTRAP_SERVER> <READ_TOPIC> <WRITE_TOPIC>')

# Extracting arguments
CHECKPOINT_DIR, BOOTSTRAP_SERVER, READ_TOPIC, WRITE_TOPIC = sys.argv[1:]

# Checking if checkpoint directory exists and deleting it if it does exist.
if os.path.exists(CHECKPOINT_DIR):
    print('Clearing checkpoint directory:', CHECKPOINT_DIR)
    shutil.rmtree(CHECKPOINT_DIR)

# Creating a Spark session with app name as given 
spark = SparkSession.builder.appName("application-for-streaming-reddit-comments").getOrCreate()

# Suppressing warnings and unwanted logging
spark.sparkContext.setLogLevel("WARN")
logging.getLogger("py4j").setLevel(logging.ERROR)

# Creating a DataFrame to represent the Kafka input stream
input_stream = spark.readStream.format("kafka")\
                   .option("kafka.bootstrap.servers", "localhost:9092")\
                   .option("subscribe", READ_TOPIC).load()\
                   .selectExpr("CAST(value AS STRING)")

# Printing the schema of the DataFrame
input_stream.printSchema()

# Assigning the DataFrame to the variable 'words'
comments = input_stream

# Defining a list of tags for parts-of-speech which are NN - noun singular, NNPS - proper noun plural, NNP - proper noun singular, NNS - noun plural
tags = ["NN", "NNPS", "NNP",  "NNS"]

#We have created a function to retrieve  named entities by filtering words based on defined tags 
@udf(returnType=StringType())
def identifyNamedEntities(inputText):
    # Tokenizing the input text
    tokens = nltk.word_tokenize(inputText)
    
    # Performing part-of-speech tagging
    pos_tags = nltk.pos_tag(tokens)
    
    # Filtering words based on their tags
    named_entities = [word for word, tag in pos_tags if tag in tags]
    
    return ','.join(named_entities)

# Applying the UDF to the DataFrame column
comments = comments.select(identifyNamedEntities("value").alias("comments"))

# Remove stop words
stp_wrds = stopwords.words('english')  # Load English stop words list
comments = comments.filter(~col("comments").isin(stp_wrds))  # Filter out stop words
comments = comments.select(explode(split(comments.comments, ',')))  # Split comma-separated words into individual rows

# Remove empty & null words
comments = comments.filter(col("comments").isNotNull() & (col("comments") != ""))  # Filter out null and empty words
comments = comments.selectExpr("col as comments")  # Rename column as 'words'

comments.printSchema()  # Print the schema of the DataFrame

# Convert words to lowercase
comments = comments.withColumn("comments", lower("comments"))\
            .withColumn("comments", regexp_replace("comments", "[^a-zA-Z0-9\\s]+", "")) \
            .withColumn("comments", trim("comments")) \
            .withColumn("count", lit(1))

# Remove empty & null words
comments = comments.filter(col("comments").isNotNull() & (col("comments") != ""))
# Print the schema of the DataFrame
comments.printSchema()

# Print the schema of the DataFrame
comments.printSchema()

# Define window size for aggregation
win_size = "5 seconds"

# Calculate word frequencies
wrd_cnt = comments.groupBy('comments').count()

# Add timestamp column truncated to seconds
wrd_cnt = wrd_cnt.withColumn("timestamp", date_trunc("second", current_timestamp()))

# Print the schema of the DataFrame
wrd_cnt.printSchema()

# Define the schema for the final topic
fnl_sch = StructType([
    StructField("comments", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("count", IntegerType(), True)
])

# Start streaming query to console

cns_que = wrd_cnt.orderBy(desc("count")).writeStream\
                    .outputMode("complete")\
                    .format("console")\
                    .start()
# Write word counts to Kafka topic
q1 = comments\
        .selectExpr("CAST(comments AS STRING) AS key",  "to_json(struct(*)) AS value") \
        .writeStream.format("kafka").option("kafka.bootstrap.servers", BOOTSTRAP_SERVER) \
        .outputMode("update") \
        .option("topic", WRITE_TOPIC).option("checkpointLocation", CHECKPOINT_DIR) \
        .start()

# Wait for the Kafka write stream to finish
q1.awaitTermination()

# Wait for the console write stream to finish
cns_que.awaitTermination()
