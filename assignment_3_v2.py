from datetime import time
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import FloatType, IntegerType
import pyspark.sql.functions as func

if __name__ == '__main__':
    spark = SparkSession.builder.appName('assignment_3').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    titanicCSV = spark.read.option('header', 'true').csv('hdfs:///user/maria_dev/assignment_3/titanic.csv')

    # 3a
    titanicCSV = titanicCSV.withColumn('Survived', titanicCSV['Survived'].cast(IntegerType()))

    titanicCSV.groupBy(['Sex', 'Pclass']) \
        .agg(func.sum("Survived").alias("Survived"), func.count("Survived").alias('Total_number_of_people')) \
        .withColumn('Percentage_of_survivors', func.round((func.col('Survived')/func.col('Total_number_of_people'))*100,2)) \
        .sort(['Sex', 'Pclass']) \
        .show()

    # 3b 
    # Ik weet niet wat al die termen als  'bayesian, beta distribution en belief distribution zijn'. 
    # Het voelt een beetje nutteloos om aan deze opdracht te beginnen.

    # 3c
    titanicCSV = titanicCSV.withColumn('Fare', titanicCSV['Fare'].cast(FloatType()))
    titanicCSV.groupBy('Pclass') \
        .avg('Fare') \
        .orderBy('Pclass', ascending = True) \
        .show()

spark.stop()