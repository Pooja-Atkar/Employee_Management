from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from common.readdatautil import ReadDataUtil

if __name__ == '__main__':

    spark = SparkSession.builder.master("local[*]") \
        .appName("Employee_Management").getOrCreate()


    # rdu = ReadDataUtil()

    empDF = spark.read.option("multiline",True).csv(r"D:\PythonSparkProject\Employee_Management\Employee_Input_Files\employee_details.csv", header=True)


# Remove \n from the address column.
    empdf = empDF.withColumn('address', regexp_replace("address", r"\n"," ")).show(truncate=False)


# save file in TargetFile (SCD 0 means No Change).
#     empDF.write.csv(r"D:\PythonSparkProject\Employee_Management\TargetFile\employee_Data.csv")
