# Classify posts (below or above 1h til accepted answer)

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.classification import NaiveBayes, RandomForestClassifier, LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ByteType, ArrayType, DoubleType,LongType, BooleanType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import VectorUDT
from pyspark.sql.functions import col, udf



# Path
path = '/user/s1978306/all_features'

# Global variables
spark = SparkSession.builder \
    .config("spark.driver.memory", "15G") \
    .config("spark.memory.offHeap.enabled", True) \
    .config("spark.memory.offHeap.size", "15G") \
    .getOrCreate()

# Show only errrors
spark.sparkContext.setLogLevel('ERROR')

# Read data
schema = StructType([
    StructField("PR", DoubleType(), True),
    StructField("ASR", DoubleType(), True),
    StructField("TCF", DoubleType(), True),
    StructField("VCF", DoubleType(), True),
    StructField("ACF", DoubleType(), True),
    StructField("latf_h", DoubleType(), True),
    StructField("RSR", DoubleType(), True),
    StructField("num_code_snippet", IntegerType(), True),
    StructField("code_len", IntegerType(), True),
    StructField("num_image", IntegerType(), True),
    StructField("body_len", IntegerType(), True),
    StructField("title_len", IntegerType(), True),
    StructField("end_que_mark", BooleanType(), True),
    StructField("begin_que_word", BooleanType(), True),
    StructField("is_weekend", BooleanType(), True),
    StructField('id', LongType()),
    StructField('label', IntegerType()),
])

df = spark.read.schema(schema).csv(path)
schema = StructType([
    StructField('id', LongType()),
    StructField('label', IntegerType()),
    StructField('features', VectorUDT()),
])

path = '/user/s1976427/CountVectors/'
#df_count = spark.read.schema(schema).json(path).drop(col('label'))
#df = df_count.join(df, df.id == df_count.id, 'inner')

df.withColumn("RSR", col("RSR")/14000000)


va = VectorAssembler().setInputCols([
    "PR",
    "ASR",
    "TCF",
    "VCF",
    "ACF",
    "latf_h",
    "RSR",
    "num_code_snippet",
    "code_len",
    "num_image",
    "body_len",
    "title_len",
    "end_que_mark",
    "begin_que_word",
    "is_weekend"
])\
      .setOutputCol("features")
naiveBayes = NaiveBayes(modelType="multinomial", labelCol="label", featuresCol="features")
rf = RandomForestClassifier(featuresCol = 'features', labelCol = 'label')
lr = LogisticRegression(maxIter=5, regParam=0.3, elasticNetParam=0.0)


pipeline = Pipeline().setStages([
    va,
    rf
])


## Fitting the model
(trainingData, testData) = df.randomSplit([0.8, 0.2], seed=100)
model = pipeline.fit(trainingData)
predictions = model.transform(testData)

evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Accuracy of rf is = %g" % (accuracy))
