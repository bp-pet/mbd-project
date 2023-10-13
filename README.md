# Managing Big Data Project
Group 4 MBD project repository.
## Research questions
-   Can we and to which extent, improve the prediction accuracy of answering time to stack overflow questions?
    -   Which features are most useful in predicting answer time on stack overflow questions?
    -   How have the answering times to stack overflow questions changed over time?

## Dataset
[Stack overflow dataset](https://archive.org/details/stackexchange)

On the hdfs, the path for the data is `/user/s1976427/Project/Posts.xml.gz`.
A truncated version (first 250 thousand posts out of 54 million) is available, called `TruncatedPosts.xml.gz`.
## Reading xml
To read XML files, we use [spark-xml](https://github.com/databricks/spark-xml). The jar is included in the code and must be added like in the example below.

    spark-submit --jars spark-xml_2.11-0.9.0.jar {script name}

## Running spark-nlp

### Step 1

Use python3

    export PYSPARK_PYTHON=/usr/bin/python3
    export PYSPARK_DRIVER_PYTHON=/usr/bin/python3

### Step 2
Install spark-nlp python package

    pip3 install spark-nlp==2.7.5

### Step 3

    spark-submit --packages com.johnsnowlabs.nlp:spark-nlp_2.11:2.7.5 SparkNlp.py

## Running project

    spark-submit --master yarn --deploy-mode cluster --conf spark.dynamicAllocation.maxExecutors=10 --jars spark-xml_2.11-0.9.0.jar DataExploration.py
