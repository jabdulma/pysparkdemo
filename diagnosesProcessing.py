from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

schema = StructType([
    StructField("row_id", StringType(), True),
    StructField("subject_id", StringType(), True),
    StructField("hadm_id", StringType(), True),
    StructField("seq_num", StringType(), True),
    StructField("icd9_code", StringType(), True)
])

def createDiagnosesDataframe(spark, dfp):
    #Convert values from byte array to string
    step1 = dfp.withColumn("valueDecode", F.col("value").cast("string"))
    #Split columns
    step2 = step1.select('valueDecode', F.split('valueDecode', ',').alias('pval'))

    #Rename columns
    step3 = step2.select([F.col("pval")[i].alias(schema[i].name) for i in range(len(schema))])
    #Take away first row
    df_result = spark.createDataFrame(step3.tail(step3.count()-1), step3.schema)
    return df_result
