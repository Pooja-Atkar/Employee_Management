from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from common.readdatautil import ReadDataUtil

if __name__ == '__main__':

    spark = SparkSession.builder.master("local[*]") \
        .appName("Employee_Management").getOrCreate()


    # rdu = ReadDataUtil()

    srcDF = spark.read.option("multiline",True).csv(r"D:\PythonSparkProject\Employee_Management\Employee_Input_Files\employee_details.csv", header=True)


# Replace (\n) by (' ').
    srcdf = srcDF.withColumn('address', regexp_replace("address", r"\n"," ")).show(truncate=False)

# Add Date
    srcDF = srcDF.withColumn("fromdate",current_date())
    # srcDF.show()
    # srcDF.cache()

# create empty dataframe for Target File

    schema = StructType([StructField("tg_emp_id",StringType()),
                         StructField("tg_fname",StringType()),
                         StructField("tg_lname",StringType()),
                         StructField("tg_age",IntegerType()),
                         StructField("tg_salary",LongType()),
                         StructField("tg_dept_id",IntegerType()),
                         StructField("tg_address",StringType()),
                         StructField("tg_city",StringType()),
                         StructField("tg_state",StringType()),
                         StructField("tg_mobile_number",IntegerType()),
                         StructField("tg_fromdate",DateType())
                         ])

    TargetDF = spark.createDataFrame([],schema=schema)
    # TargetDF.show()

#  Left outer join on source and target
    empDF = srcDF.join(TargetDF,srcDF.emp_id == TargetDF.tg_emp_id,'left')
    # empDF.show()

#  flag the record for insert or update

# Insert flag

    scd_df = empDF.withColumn("insertflag",when((empDF.emp_id != empDF.tg_emp_id)|
                              empDF.emp_id.isNull(),'Y').otherwise('NA'))

    # scd_df.show()

# Update flag
    scd_DF1 = scd_df.withColumn("updateflag",when(scd_df.emp_id == scd_df.tg_emp_id,'Y').otherwise('NA'))

    # scd_DF1.show()


# save file in TargetFile (SCD 0 means No Change).
#     empDF.write.csv(r"D:\PythonSparkProject\Employee_Management\TargetFile\employee_Data")

