from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
#import spark-csv

# Use pyspark to convert a CSV file to ORC

sc = SparkContext(appName="CSV2Parquet")
sqlContext = SQLContext(sc)


customSchema = StructType([
 
        StructField("field1", LongType(), True),
        StructField("field2", LongType(), True),
        StructField("field3", StringType(), True),
        StructField("field4", StringType(), True),
        StructField("field5", StringType(), True),
        StructField("field6", ShortType(), True),
        StructField("field7", LongType(), True),
        StructField("field8", StringType(), True),
        StructField("field9", StringType(), True),
        StructField("field10", StringType(), True),
        StructField("field11", LongType(), True),
        StructField("field12", StringType(), True),
        StructField("field13", StringType(), True),
        StructField("field14", LongType(), True),
        StructField("field15", LongType(), True),
        StructField("field16", StringType(), True),
        StructField("field17", StringType(), True),
        StructField("field18", LongType(), True),
        StructField("field19", StringType(), True),
        StructField("field20", StringType(), True),
        StructField("field21", LongType(), True),
        StructField("field22", StringType(), True),
        StructField("field23", StringType(), True),
        StructField("field24", LongType(), True),
        StructField("field25", StringType(), True),
        StructField("field26", StringType(), True),
        StructField("field27", StringType(), True),
        StructField("field28", StringType(), True),
        StructField("field29", DoubleType(), True),
        StructField("field30", StringType(), True),
        StructField("field31", LongType(), True)

  
])

print "Creating data frame from the CSV file \n" 

df = sqlContext.read \
    .format('com.databricks.spark.csv') \
    .options(header='false', nullValue=r'\N') \
    .load("/Users/tweng/Documents/CSVtoORC/CSV_Inputs/sample.csv", schema = customSchema)


#df.write.orc('/Users/tweng/Documents/CSVtoORC/output_file/')
df.write.format("orc").option("compression", "snappy").save("/Users/tweng/Documents/CSVtoORC/ORC_Outputs/")



#df4.write.format("com.databricks.spark.csv").option("header", "false").option("quote", " " ).save("/output/path")