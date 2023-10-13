# Script to merge the tag based and non tag based features

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

start_year = 2007
end_year = 2050

df1 = df.select(col('_Id').alias('id')\
        ,(col('_CreationDate')[0:4].cast('int')).alias('year')\
	,col('_Tags').alias('tags')\
        )
# filter null values for the 
df1 = df1.filter(df1.tags.isNotNull())
# 'Explode the dataframe' by turning it into an rdd and back to an dataframe
rdd1 = df1.rdd
def filter_tags(str): return str[1:-1].split("><")

rdd2 = rdd1.flatMap(lambda row: [(tag,(row[0], row[1])) for tag in filter_tags(row[2])])
df1 = rdd2.toDF()
df1 = df1.select(col('_1').alias('tags')\
	,(col('_2')['_1']).alias('id')\
	,(col('_2')['_2']).alias('year')\
	)
# Filter on the specific years
df2 = df1.filter(col('year').between(start_year, end_year))

path_rsr = '/user/s1978306/rsr2'
path_asr_pr = '/user/s1978306/asr_pr'
path_four = '/user/s1978306/four'
#----------------------------------------------
df_rsr = spark.read.csv(path_rsr)
df_rsr = df_rsr.select(col('_c0').alias('tags_rsr')\
        ,col('_c1').alias('ID')\
        ,col('_c2').alias('t_diff')\
        )
# do the rest that was not possible in the other file
df_rsr = df_rsr.filter(df_rsr.t_diff< 1)
df_rsr = df_rsr.groupBy('tags_rsr').count()\
       .withColumnRenamed('count', 'RSR')

df_asr_pr = spark.read.csv(path_asr_pr)
df_asr_pr = df_asr_pr.select(col('_c0').alias('tags_asr_pr')\
	,col('_c1').alias('PR')\
	,col('_c2').alias('ASR')\
	)


df_four = spark.read.csv(path_four)
# Get the correct column names
df_four = df_four.select(col('_c0').alias('tags_four')\
	,col('_c1').alias('TCF')\
	,col('_c2').alias('VCF')\
	,col('_c3').alias('ACF')\
	,col('_c4').alias('latf_h')\
	)

df_merged = df_asr_pr.join(df_four, df_asr_pr.tags_asr_pr == df_four.tags_four, 'inner')
df_merged = df_merged.drop('tags_asr_pr')
df_merged = df_merged.join(df_rsr, df_rsr.tags_rsr == df_merged.tags_four, 'inner')

df_merged = df_merged.drop('tags_rsr')

# to merge everything, just filter I guess
# keep on creating a temporary dataframe
# or use the 'where' function


# join the dataframes 

joined_final = df2.join(df_merged, df_merged.tags_four == df2.tags, 'inner')
# Drop the tags_four tag and get the average (check out stack overflow post)
joined_final = joined_final.drop('tags_four')

# Cast everything to int/long

joined_final2 = joined_final.select(col('tags')\
	,col('id').alias('id').cast('long')\
	,(col('PR') * 1000 /(14 * 10 ** 6) ).alias('PR').cast('double')\
	,(col('ASR') * 1000 / (14*10**6)).alias('ASR').cast('double')\
	,col('TCF').cast('double')\
	,col('VCF').cast('double')\
	,col('ACF').cast('double')\
	,col('latf_h').cast('double')\
	,col('RSR').cast('double')\
	)
joined_final2 = joined_final2.groupBy('tags')\
	.avg('PR', 'ASR', 'TCF', 'VCF', 'ACF', 'latf_h', 'RSR')

# multiply ASR and PR by 1000, then divide by the total number of users

path_to_write = "/user/s1978306/merged_data3"
joined_final2.coalesce(1).write.format('csv').save(path_to_write)
