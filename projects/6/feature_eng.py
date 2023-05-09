#!/opt/conda/envs/dsenv/bin/python
import os, sys
import logging

logging.basicConfig(level=logging.DEBUG)
logging.info("CURRENT_DIR {}".format(os.getcwd()))
logging.info("SCRIPT CALLED AS {}".format(sys.argv[0]))
logging.info("ARGS {}".format(sys.argv[1:]))

from pyspark.sql import SparkSession
from pyspark.sql.types import *

from pyspark.ml.feature import *
from pyspark.ml.regression import GBTRegressor
from pyspark.ml import Pipeline

from pyspark.ml.functions import vector_to_array
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

path_in, path_out = sys.argv[1], sys.argv[2]

if "test" in path_out:
    test = True
else:
    test = False

train_schema = StructType([
    StructField("label", DoubleType()),
    StructField("vote", DoubleType()),
    StructField("verified", BooleanType()),
    StructField("reviewTime", StringType()),
    StructField("reviewerID", StringType()),
    StructField("asin", StringType()),
    StructField("id", LongType()),
    StructField("reviewerName", StringType()),
    StructField("reviewText", StringType()),
    StructField("summary", StringType()),
    StructField("unixReviewTime", TimestampType())
])

test_schema = StructType([
    StructField("label", DoubleType()),
    StructField("verified", BooleanType()),
    StructField("reviewTime", StringType()),
    StructField("reviewerID", StringType()),
    StructField("asin", StringType()),
    StructField("id", LongType()),
    StructField("reviewerName", StringType()),
    StructField("reviewText", StringType()),
    StructField("summary", StringType()),
    StructField("unixReviewTime", TimestampType())
])

if test:
    schema = test_schema
else:
    schema = train_schema

data = spark.read.json(path_in, schema=schema)

data = data.fillna({'summary':''})
data.cache()

tokenizer_rt = RegexTokenizer(inputCol="reviewText", outputCol="words_rt", pattern="\\W")

stop_words = StopWordsRemover.loadDefaultStopWords("english")

swr_rt = StopWordsRemover(inputCol=tokenizer_rt.getOutputCol(),
                          outputCol="words_filtered_rt", stopWords=stop_words)

hasher_rt = HashingTF(numFeatures=100, binary=True,
                      inputCol=swr_rt.getOutputCol(), outputCol="word_vector_rt")

assembler = VectorAssembler(inputCols=['word_vector_rt'], outputCol="word_vector")

pipeline = Pipeline(stages=[
    tokenizer_rt,
    swr_rt,
    hasher_rt,
    assembler
])

df = pipeline.fit(data).transform(data)
df = df.withColumn("features", vector_to_array("word_vector"))

if test:
    df.select("id", "features").write.mode("overwrite").save(path_out)
else:
    df.select("label", "features").write.mode("overwrite").save(path_out)
