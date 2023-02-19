from pyspark.sql import SparkSession
import pyspark
import logging
import logging.config
from pyspark.sql.functions import col
from pyspark.sql.functions import *
import sys
#
class Transform:
    logging.config.fileConfig("resource/configs/logging.conf")

    def __init__(self,spark):
        self.spark = spark

    def transform_data(self, df):
        logger = logging.getLogger("Transform")
        try:
            logger.info("Transforming data")
            # Drop all the rows having null values (df1 = df.na.drop())
            df1 = df.na.fill("Unknown",['author_name'])
            df2 = df1.na.fill("0", ['no_of_reviews'])

            df2.show()

            #  More transformer option
            # df1.select("Age").show()
            # df1.select("*").groupBy("Gender").agg({"salary":"sum","age":"max"}).show()
            # df1.select("*").groupBy("Gender").agg(sum(col("salary")).alias("total_salary"),max("age").alias("max_age")).show()
            # df1.where("Salary > 30000").show()
            #
            # # Transforming using Temp View
            # logger.info("Transforming using Temp View")
            # df1.createOrReplaceTempView("temp_view")
            # self.spark.sql("select * from temp_view")
            return df1

        except Exception as exp:
            logger.warning("Cant execute code" + str(exp))
            sys.exit(1)


