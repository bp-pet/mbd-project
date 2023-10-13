# Compute the non-tag based features.

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,unix_timestamp, udf
from pyspark.sql.types import IntegerType

from datetime import datetime


# Constants
readFormat = 'json'
truncated = False

# Paths
basePath = '/user/s1976427/'
# jsonDataPath = basePath + ('Truncated' if truncated else '') + 'Posts/'
jsonDataPath = basePath + 'PostsLast4Years/'
xmlDataPath = basePath + ('Truncated' if truncated else '') + 'Posts.xml.gz'

# Global variables
spark = SparkSession.builder.getOrCreate()

# Read
if readFormat == 'xml':
    df = spark.read\
        .format('com.databricks.spark.xml')\
        .options(rowTag='row')\
        .load(xmlDataPath)
else:
    df = spark.read.\
        json(jsonDataPath)

# Define functions
udf_len = udf(lambda x: len(x), IntegerType())
udf_code_snippets = udf(lambda x: len(x.split("<code")) - 1)
udf_code_length = udf(lambda x: sum(len(i.split("</code>")[0]) for i in x.split("<code")[1:]))
udf_images = udf(lambda x: len(x.split("<img")) - 1)
udf_end_question = udf(lambda x: x[-1] == "?")
udf_start_wh = udf(lambda x: x[0:2].lower() == "wh")
udf_is_weekend = udf(lambda x: datetime(int(x[0:4]), int(x[5:7]), int(x[8:10]), int(x[11:13]), int(x[14:16]), int(x[17:19])).weekday() in [5, 6])


# Create features


df = df.select(col("_PostTypeId").alias("type"),
		col("_Id").alias("id"),
		col("_Title").alias("title"),
		col("_Body").alias("body"),
		col("_CreationDate").alias("creation_date"),
		(col("_CreationDate")[0:4].cast('int')).alias('creation_year')) # take only relevant columns

df = df.filter(df.type == 1) # filter only questions

df = df.withColumn("num_code_snippet", udf_code_snippets(df.body))\
	.withColumn("code_len", udf_code_length(df.body))\
	.withColumn("num_image", udf_images(df.body))\
	.withColumn("body_len", udf_len(df.body))\
	.withColumn("title_len", udf_len(df.title))\
	.withColumn("end_que_mark", udf_end_question(df.title))\
	.withColumn("begin_que_word", udf_start_wh(df.title))\
	.withColumn("is_weekend", udf_is_weekend(df.creation_date))

df = df.drop("body").drop("title").drop("type").drop("creation_date").drop("creation_year")

#df.show(500)

# Write to a file
df.write.option("header",True)
path_to_write = "/user/s2020343/ntb"
df.coalesce(80).write.mode('append').format('csv').save(path_to_write)
