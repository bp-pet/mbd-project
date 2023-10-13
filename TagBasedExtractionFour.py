from pyspark.sql import SparkSession
from pyspark.sql.functions import col,unix_timestamp, collect_list
import time

from datetime import datetime # to convert a unix timestamp to a year

# Constants
readFormat = 'json'
truncated = False

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

# The first 3 features RSR, ASR and PR can be added later
# as they are specific for a small part on the data (4 years orso)

df1 = df.select(col('_Tags').alias('tags')\
	,col('_ViewCount').alias('viewcount')\
	,col('_AnswerCount').alias('answercount')\
	,(unix_timestamp(col('_LastActivityDate').cast('timestamp')) -
	 unix_timestamp(col('_CreationDate').cast("timestamp")))\
	.alias('latf (s)')\
	)

# we do not round to hours yet, we will parse the difference to an int, and then parse it to hours later.
# this is because for some reason the function "toDF" does not copy double values, only long or ints

# Replace null values for the answercount and the viewcount with 0 and
# remove questions that do not have tags
df1 = df1.filter(df1.tags.isNotNull())
df1 = df1.fillna({'viewcount': 0, 'answercount': 0})
# Turn into an rdd to use reduceByKey and add the counts per tag
tags_rdd = df1.rdd
def filter_tags(str): return str[1:-1].split("><")

tags_rdd1 = tags_rdd.flatMap(lambda row: [(tag,(1, row[1], row[2], long(row[3]))) for tag in filter_tags(row[0])])
# Add data together based on tags
tags_rdd2 = tags_rdd1.reduceByKey(lambda a, b: map(sum, zip(a, b)))
# Back to a df
df2 = tags_rdd2.toDF()
# now use split with column to get the correct format
df3 = df2.select(col("_1").alias("tags")\
	,col("_2")[0].alias("TCF")\
	,col("_2")[1].alias("VCF")\
	,col("_2")[2].alias("ACF")\
	,(col("_2")[3]/3600).alias("latf_h")\
	)

df.write.option("header",True)

#path_to_write = "/user/s1978306/features/four/"
#df.write.scv("/user/s1978306/features/four.csv"')
path_to_write = "/user/s1978306/four"
df3.coalesce(1).write.format('csv').save(path_to_write)



# test
df1 = df.select(unix_timestamp(col('_CreationDate').cast('timestamp')).alias('time'))
