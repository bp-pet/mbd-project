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

# The Responsive subscribers ratio is more complicated and deserves special attention
def filter_tags(str): return str[1:-1].split("><")

rsr_df = df.select(col('_Tags').alias('tags')\
        ,col('_Id').alias('id')\
        ,unix_timestamp(col('_CreationDate').cast('timestamp')).alias('time')\
        ,col('_PostTypeId').alias('postID')\
        ,col('_ParentId').alias('parentID')\
	,(col('_CreationDate')[0:4].cast('int')).alias('year')\
        )

# Filter on questions asked from 2017 onwards
rsr_df = rsr_df.filter(rsr_df.year >= 2017)
rsr_df = rsr_df.drop('year')

# Split the dataframe into questions and answers
# For the answers, drop the tags column, for the questions drop the postID column
rsr_df_q = rsr_df.filter(rsr_df.parentID.isNull())\
        .drop('postID')\
        .drop('parentID')\
        .withColumnRenamed('time', 'time_q')

rsr_df_a = rsr_df.filter(rsr_df.parentID.isNotNull())\
        .drop('tags')\
        .drop('id')\
        .withColumnRenamed('time','time_a')\
        .drop('postID')
# no need to filter out the null tags, because they do not exist anymore
# a question HAS TO CONTAIN A TAG

# Now we can move to an rdd to 'unpack' the tags
# we will do this by converting them to an rdd

# remove all null tags for questions
rsr_df_q = rsr_df_q.filter(rsr_df_q.tags.isNotNull())

rdd_q = rsr_df_q.rdd

rdd_q = rdd_q.flatMap(lambda row: [(tag,(row[1], long(row[2]))) for tag in filter_tags(row[0])])
rsr_df_q2 = rdd_q.toDF()

rsr_df_q2 = rsr_df_q2.select(col('_1').alias('tags')\
        ,(col('_2')['_1']).alias('ID')\
        ,(col('_2')['_2']).alias('time_q')\
        )

# perform an inner join
result_df = rsr_df_q2.join(rsr_df_a, rsr_df_q2.ID == rsr_df_a.parentID, 'inner')

result_df2 = result_df.select(col('tags')\
        ,col('ID')\
        ,((col('time_a') - col('time_q')) / 3600).alias('t_diff')\
        )

# Now filter the comments that were posted in less than 1 hour

#result_df2 = result_df2.filter(result_df2.t_diff < 1)

# test = result_df3.orderBy('ID', ascending = True)

#result_df2 = result_df2.groupBy('tags').count()\
#	.withColumnRenamed('count', 'RSR')

# Write to a file
path_to_write = "/user/s1978306/rsr2"
result_df2.write.mode('overwrite').format('csv').save(path_to_write)
