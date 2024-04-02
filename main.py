from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from patientProcessing import createPatientDataframe
from diagnosesProcessing import createDiagnosesDataframe
from icdCodeProcessing import createICDCodeDataframe

from dotenv import load_dotenv
import os

load_dotenv()
kAddress = os.getenv("kafkaAddress")
print("Kafka Address: " + kAddress)

spark = SparkSession \
    .builder \
    .appName("Streaming from Kafka") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3') \
    .config("spark.sql.shuffle.partitions", 4) \
    .getOrCreate()

print('The PySpark version is: ' + spark.version)
print('The internal Scala is: ' + spark.sparkContext._gateway.jvm.scala.util.Properties.versionString())

patient_df = spark \
  .read \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kAddress) \
  .option("subscribe", "rawdata-patient") \
  .load()
patient_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

processedPatientDf = createPatientDataframe(spark, patient_df)

diagnoses_df = spark \
  .read \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kAddress) \
  .option("subscribe", "rawdata-diagnoses") \
  .load()
diagnoses_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

processedDiagnosesDf = createDiagnosesDataframe(spark, diagnoses_df)

icd_codes_df = spark \
  .read \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kAddress) \
  .option("subscribe", "rawdata-icdcodes") \
  .load()
icd_codes_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

processedICDCodes = createICDCodeDataframe(spark, icd_codes_df)

# Uncomment to see processed tables in log.
#processedPatientDf.show(5)
#processedDiagnosesDf.show(5)
#processedICDCodes.show(5)


#Example dataset to get a patient's gender, dob, procedure code, and description.
bigDf = processedPatientDf.alias("a") \
  .join(processedDiagnosesDf.alias("b"), col("b.subject_id") == col("a.subject_id")) \
  .join(processedICDCodes.alias("c"), col("c.icd9_code") == col("b.icd9_code")) \
  .select(
      "a.subject_id",
      "a.gender",
      "a.dob",
      "b.icd9_code",
      "c.short_title"
  )

bigDf.show(20)

#Example of writing to console, replace with writing out to kafka, file, DB, etc.

#writing_df = bigDf.write \
#    .format("console") \
#    .mode("append") \
#    .save()
