import os
import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import logging
from logging.handlers import RotatingFileHandler

def setup_logger():

    # Initiate Logging
    logger = logging.getLogger('Logger_kummatipara')

    logger.setLevel(logging.DEBUG)


    handler = RotatingFileHandler('./logs/join_dataset.log', maxBytes=2000, backupCount=10)
    handler.setLevel(logging.DEBUG)

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # add formatter to rotating file handler
    handler.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(handler)

    return logger






def initiate_spark_connection():
    """
    Initiate the Spark Connection.

    This function creates a spark connection.

    :return: Spark Connection
    :rtype: object
    """
    
    sc = SparkSession.builder.appName("Pyspark Exercise").config("spark.driver.host", "localhost").getOrCreate()
    
    return sc
   

def remove_info(df, l):
    
    df = df.drop(*l)

    return df

def rename_columns(df, names):

    for old_name, new_name in names.items():
        df = df.withColumnRenamed(old_name, new_name)

    return df


if __name__ == "__main__":

    logger = setup_logger()
    
    filter_list = []
    n = len(sys.argv)


    path_1 = sys.argv[1]
    path_2 = sys.argv[2]
    filter_list = sys.argv[3:n]
    
    print(filter_list)
    output_path = "./client_data/"

    new_cols = {
    "id" : "client_identifier",
    "btc_a" : "bitcoin_address",
    "cc_t": "credit_card_type"
    }

    pii = ["first_name", "last_name"]
    sensitive_info = ['cc_n']

    logger.info('Initiating Spark Connection')

    sc = initiate_spark_connection()

    logger.info('Reading Customer Data')
    df1 = sc.read.csv(path_1, header=True)
    logger.info("Loaded DataFrame with {} rows.".format(df1.count()))

    logger.info('Reading Bitcoin Data')
    df2 = sc.read.csv(path_2, header=True)
    logger.info("Loaded DataFrame with {} rows.".format(df1.count()))

    logger.info('Filtering Countries and Removing PIIs')
    df1 = df1.filter(df1.country.isin(filter_list))

    df1 = remove_info(df1, pii)
    df2 = remove_info(df2, sensitive_info)

    logger.info('Creating the final dataset')

    result = df1.join(df2, on="id", how="left")
    
    result = rename_columns(result, new_cols)

    logger.info('Saving info into CSV')

    result.write.format("csv").option("header", "true").mode("overwrite").save(output_path)

    sc.stop()

    

    

