# PySpark Demo

This is a simple [PySpark](https://pypi.org/project/pyspark/) demo to show how you could load data from a [Kafka](https://kafka.apache.org/) topic into [Spark](https://spark.apache.org/) dataframes, and then finally perform a simple transform of joining the datasets together.  The end result shows some patient records with their anonymized "Subject Id", then their gender, date of birth, icd9 code for their diagnosis, and finally the short description of that code.

This demo was created using the publicly available [Mimic-III clinical database demo](https://physionet.org/content/mimiciii-demo/1.4/).  Specifically the `Patients`, `Diagnoses_Icd`, and `D_icd_diagnoses` datasets are used.  They are loaded into kafka, using the file configs provided in the `kafka_configs` directory.  The files themselves are provided in the `mimic-iii` directory.

## To use
This software was tested with `Python 2.7.17` and `Pyspark 3.0.3` with internal scala version `2.12.10`.  Data was loaded into Kafka version `kafka_2.13-3.7.0`.   

Steps:
- Clone this repo to have the kafka configs and data files ready.
- Install Kafka and load data into it using the provided configuration files in `kafka_configs`.  Instructions to do so can be found in the [Kafka Quickstart Guide](https://kafka.apache.org/quickstart).
- Install Python and PySpark versions listed above.  As well as the `dotenv` package.
-  Create a `.env` file with the following line
	- `kafkaAddress="hostnameOrIp:port"` - replacing the hostname and port to reflect your environment
- To run the file, run `python main.py`.
- Once the file completes, the first 20 lines of the combined tables will be shown in the console.  If successful it will look like the following:
```
+----------+------+-------------------+---------+--------------------+
|subject_id|gender|                dob|icd9_code|         short_title|
+----------+------+-------------------+---------+--------------------+
|     10045|     F|2061-03-25 00:00:00|     0383|Anaerobic septicemia|
|     41976|     M|2136-07-28 00:00:00|     0383|Anaerobic septicemia|
|     41976|     M|2136-07-28 00:00:00|    03840|Gram-neg septicem...|
|     41976|     M|2136-07-28 00:00:00|    03849|Gram-neg septicem...|
|     41976|     M|2136-07-28 00:00:00|    03849|Gram-neg septicem...|
|     41914|     M|2090-11-16 00:00:00|    03849|Gram-neg septicem...|
|     40655|     F|1844-07-18 00:00:00|     0388|      Septicemia NEC|
|     10106|     M|2097-12-16 00:00:00|     0543|Herpetic encephal...|
|     10059|     M|2081-01-03 00:00:00|    07070|Hpt C w/o hepat c...|
|     10059|     M|2081-01-03 00:00:00|    07070|Hpt C w/o hepat c...|
|     41976|     M|2136-07-28 00:00:00|     1120|              Thrush|
|     10117|     F|2072-05-05 00:00:00|     1120|              Thrush|
|     10045|     F|2061-03-25 00:00:00|     1179|   Mycoses NEC & NOS|
|     43881|     M|2051-03-24 00:00:00|     1508|Mal neo esophagus...|
|     43881|     M|2051-03-24 00:00:00|     1508|Mal neo esophagus...|
|     10065|     F|2111-07-18 00:00:00|     1510|Mal neo stomach c...|
|     43798|     M|2136-07-29 00:00:00|     1510|Mal neo stomach c...|
|     44083|     M|2057-11-15 00:00:00|     1510|Mal neo stomach c...|
|     43746|     F|2029-12-07 00:00:00|     1541|Malignant neopl r...|
|     44228|     F|2112-10-22 00:00:00|     1561|Mal neo extrahepa...|
+----------+------+-------------------+---------+--------------------+
```

## Next steps as time allows
- Package requirements in `requirements.txt`

