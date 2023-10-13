from pyspark.sql import SparkSession
from pyspark.sql.functions import col,unix_timestamp, collect_list
import time

from datetime import datetime # to convert a unix timestamp to a year

# Constants
readFormat = 'json'
truncated = True

# Paths
basePath = '/user/s1976427/'
jsonDataPath = basePath + ('Truncated' if truncated else '') + 'Posts/'
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
# Extract the ASR and PR similarly as the 4 new tags by Johnson, Foster, Li, Elmiligi and Rahman

df1 = df.select(col('_Tags').alias('tags')\
	,(col('_AnswerCount') + col('_CommentCount')).alias('responseCount')\
	,(col('_CreationDate')[0:4].cast('int')).alias('year')\
	)
# Filter on questions that are asked from 2017 onwards
df1 = df1.filter(df1.year >= 2008)
df1 = df1.drop('year')
# Filter null values for the tags (which are answers)

df1 = df1.filter(df1.tags.isNotNull())
# put 0 at questions that do not have comments and/or answers
df1 = df1.fillna({'responseCount': 0})

# Turn into an rdd to use reduceByKey and add the counts per tag
response_rdd = df1.rdd
def filter_tags(str): return str[1:-1].split("><")

rdd = response_rdd.flatMap(lambda row: [(tag,(1, row[1])) for tag in filter_tags(row[0])])

rdd = rdd.reduceByKey(lambda a, b: map(sum, zip(a, b)))
# Back to a df
df1 = rdd.toDF()

# total number of users is still unkown
# but we will just initialize it to some constant
total_num_users = 14 * 10 **6

df1 = df1.select(col('_1').alias('tag')\
	,(col('_2')[0] / total_num_users).alias('PR')\
	,(col('_2')[1] / total_num_users).alias('ASR')\
	)


# TODO: write to a path
df.write.option("header",True)
path_to_write = "/user/s1978306/asr_pr"
df1.coalesce(1).write.format('csv').save(path_to_write)
