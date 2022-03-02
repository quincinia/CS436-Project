# For reading data from the dataset

import pyspark
from pyspark.sql.types import *

videoSchema = StructType([
    StructField("videoID", StringType()),
    StructField("uploader", StringType()),
    StructField("age", IntegerType()),
    StructField("category", StringType()),
    StructField("length", IntegerType()),
    StructField("views", IntegerType()),
    StructField("rate", FloatType()),
    StructField("ratings", IntegerType()),
    StructField("comments", IntegerType()),
    StructField("relatedIDs", ArrayType(StringType()))
])