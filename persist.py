import pyspark
from pyspark.sql import SparkSession
import logging
import logging.config
import sys
import psycopg2

class Persist:
    logging.config.fileConfig("resource/configs/logging.conf")
    def __init__(self,spark):
        self.spark = spark

    def write_to_csv(self, df):
        logger = logging.getLogger("Persist")

        try:
            # Apply Persist Logger config setting in logging.conf
            logger.info("Persisting data")
            df.write.format("csv")\
                .option("header","true")\
                .save("transformed_retailstore.csv")

        except Exception as exception:
            logger.error("An error occur while persisting data > " + str(exception))
            # send email notifications or save to database

            # Raise Exception for main code catch == sys.exit(1). Khi raise Exception thì app will exit.
            raise Exception("HDFS Directory already exists")

    def write_to_postgres_psycopg2(self):
        connection = psycopg2.connect(user='postgres', password='1011997', host='localhost', database='yamamoto')
        cursor = connection.cursor()
        insert_query = "INSERT INTO yamamoto_schema.yamamoto_table (course_id, course_name, author_name, course_section, creation_date) VALUES (%s, %s, %s, %s,%s)"
        insert_tuple = (30, 'Machine Learning', 'FutureX', '{}', '2020-10-20')
        cursor.execute(insert_query, insert_tuple)
        cursor.close()
        connection.commit()

    def write_to_postgres_jdbc(self,df):
        df.write.format("jdbc")\
            .option("url","jdbc:postgresql://localhost:5432/yamamoto")\
            .option("dbtable","yamamoto_schema.course_table1")\
            .option("user","postgres")\
            .option("password","1011997")\
            .save()
