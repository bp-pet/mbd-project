# Extract statistics from the data.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, add_months, avg
import matplotlib
import matplotlib.pyplot as plt

# Constants
MONTHS_IN_YEAR = 12
timeIntervals = [0, 1, 4, 12]
nrIntervals = len(timeIntervals)

# Path
path = '/user/s1976427/ResponseTimes'

# Timestamp 4 years ago
fourYearsAgoTimestamp = '2018-01-16T12:00:00.000'

# Global variables
spark = SparkSession.builder.getOrCreate()

# Show only errrors
spark.sparkContext.setLogLevel('ERROR')

# Read data
df = spark.read.json(path)

# Get total number of answered posts
print('Total number of answered posts: {}'.format(df.count()))

# Get last 4 years
df = df.filter(col('creationDate') >= fourYearsAgoTimestamp) \
    .orderBy(col('creationDate').desc())

df = df.select(col('responseTime')).filter(col('responseTime') <= 25)

pdf = df.toPandas()

pdf.hist(bins=100)

# Add labels
plt.title('Histogram of Response Times')
plt.xlabel('Response time (hours)')
plt.ylabel('Number of posts')

plt.savefig('output.png')
