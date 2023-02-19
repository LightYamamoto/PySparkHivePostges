import ingest
import transform
import persist
from pyspark.sql import SparkSession
import logging
# call function to read file config
import logging.config
import sys
class PipeLine:
    # logging.basicConfig(level="INFO")
    logging.config.fileConfig('resource/configs/logging.conf')

    def run_pipeline(self):
        logger = logging.getLogger("sLogger")

        try:
            logger.info("Running Pipeline")

            #  Ingest processing
            # self.spark = self.create_spark_session()
            self.ingest = ingest.Ingest(self.spark)

            # Read course data from Hive
            # df = self.ingest.read_from_hive()
            # # Read data from Postgres using Psycopg2
            # df = self.ingest.read_from_postgres_psycopg2()

            # # Read data from Postgres using JDBC driver
            df = self.ingest.read_from_postgres_using_jdbc()


            # Transform processing
            # self.transform = transform.Transform(self.spark)
            # transformed_df = self.transform.transform_data(df)

            # #  Persist Processing
            # self.persist = persist.Persist(self.spark)
            # Save data to csv
            # self.persist.write_to_csv(transformed_df)

            # Save data to Postgres using spycopg2
            # self.persist.write_to_postges_psycopg2()

            # Save data to postgres using JDBC Driver
            # self.persist.write_to_postgres_jdbc(transformed_df)
            return

        except Exception as exp:
            logger.error("An error throw while running the pipeline >" + str(exp))
            sys.exit(1)

    def create_spark_session(self):
        print("Creating Spark Session")
        self.spark = SparkSession.builder.appName("Demo")\
            .master("local[*]")\
            .enableHiveSupport()\
            .getOrCreate()
        return self.spark

    def create_spark_session_jdbc(self):
        print("Creating Spark Session")
        self.spark = SparkSession.builder.appName("My First Spark App")\
            .master("local[*]")\
            .config("spark.driver.extraClassPath","postgresql-42.5.4.jar") \
            .enableHiveSupport() \
            .getOrCreate()

        return self.spark

    def create_hive_table(self):
        logger = logging.getLogger("sLogger")
        logger.info(" Create_hive_table methob started")
        self.spark.sql("create database if not exists yamamoto_db")
        self.spark.sql("create table if not exists yamamoto_db.yamamoto_table (couse_id string, course_name string, author_name string, no_of_reviews string) ")
        self.spark.sql("insert into yamamoto_db.yamamoto_table VALUES (1,'Big Data Hive','Nick',100)")
        self.spark.sql("insert into yamamoto_db.yamamoto_table VALUES (2,'Java','FutureXSkill',56)")
        self.spark.sql("insert into yamamoto_db.yamamoto_table VALUES (3,'Big Data','Future',100)")
        self.spark.sql("insert into yamamoto_db.yamamoto_table VALUES (4,'Linux','Future',100)")
        self.spark.sql("insert into yamamoto_db.yamamoto_table VALUES (5,'Microservices','Future',100)")
        self.spark.sql("insert into yamamoto_db.yamamoto_table VALUES (6,'CMS','',100)")
        self.spark.sql("insert into yamamoto_db.yamamoto_table VALUES (7,'Python','FutureX','')")
        self.spark.sql("insert into yamamoto_db.yamamoto_table VALUES (8,'CMS','Future',56)")
        self.spark.sql("insert into yamamoto_db.yamamoto_table VALUES (9,'Dot Net','FutureXSkill',34)")
        self.spark.sql("insert into yamamoto_db.yamamoto_table VALUES (10,'Ansible','FutureX',123)")
        self.spark.sql("insert into yamamoto_db.yamamoto_table VALUES (11,'Jenkins','Future',32)")
        self.spark.sql("insert into yamamoto_db.yamamoto_table VALUES (12,'Chef','FutureX',121)")
        self.spark.sql("insert into yamamoto_db.yamamoto_table VALUES (13,'Go Lang','',105)")

        #Treat empty strings as null
        self.spark.sql("alter table yamamoto_db.yamamoto_table set tblproperties('serialization.null.format'='')")

        logger.info(" Hive table had been created")

        # logger.info(" Querying from Hive Database")


if __name__ == "__main__":
    logger = logging.getLogger("sLogger")
    logger.info("Running Pipeline 1")

    # Run Pipeline
    pipeline = PipeLine()
    # pipeline.create_spark_session()
    pipeline.create_spark_session_jdbc()

    # pipeline.create_hive_table()
    pipeline.run_pipeline()

