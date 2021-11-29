import sys
import os
import logging
import boto3
import datetime

from datetime import date
from io import StringIO
from pyspark.sql.functions import lit
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from urllib.parse import urlparse,parse_qs

logFormatter = '%(levelname)s - %(message)s'
logging.basicConfig(format=logFormatter, level=logging.INFO)
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

referrer_list = []
search_engine_list = []
revenue_list = []
keyword_list = []

TARGET_DATABASE = 'sample_database'
TARGET_TABLE = 'final_table'
INPUT_PATH = 's3://yavula-dev/my_sample_data/data.tsv'
TARGET_PATH = 's3://yavula-dev/my_sample_output/'
TARGET_BUCKET = 'yavula-dev'
TARGET_PREFIX = 'my_sample_output/'
TARGET_FILE = 's3://yavula-dev/sample_tsv_data/'

def read_input_file_from_s3(INPUT_PATH):
    tsv_data = spark.read.csv(INPUT_PATH, sep=r'\t', header=True)
    return tsv_data

def select_purchased_ips(tsv_data):
    order_complete_df = tsv_data.filter(tsv_data.pagename=='Order Complete')
    order_complete_ips = order_complete_df.select('ip').rdd.flatMap(list).collect()
    return order_complete_ips,order_complete_df

def collect_referrer(order_complete_ips,tsv_data):
    for order_complete_ip in order_complete_ips:
        referrer_df = tsv_data.filter(tsv_data.ip==order_complete_ip)
        referrer_df.createOrReplaceTempView("df_view")
        referrer_df=spark.sql("""
        SELECT * 
        FROM df_view ORDER BY hit_time_gmt limit 1
        """)
        referrer_entry = referrer_df.select('referrer').rdd.flatMap(list).collect()[0]
        referrer_list.append(referrer_entry)
    return referrer_list

def extract_search_engine(referrer_list):
    for referrer_entry in referrer_list:
        url = urlparse(referrer_entry) #parses the url
        search_engine_list.append(url.netloc) #netloc extracts the search engine name from the url
    return search_engine_list

def extract_keyword(referrer_list):
    for referrer_entry in referrer_list:
        url = urlparse(referrer_entry)
        query = parse_qs(url.query)
        if 'q' in query:
            keyword_list.append(query['q'][0]) #for bing(msn),google
        else:
            keyword_list.append(query['p'][0]) #for yahoo
    return keyword_list

def extract_revenue(order_complete_df):
    product_list = order_complete_df.select('product_list').rdd.flatMap(list).collect()
    for product in product_list:
        revenue = product.split(";")
        revenue_extra_step = revenue[3]
        revenue_list.append(revenue_extra_step)
    return revenue_list

def create_final_df(search_engine_list,revenue_list,keyword_list):
    answer_lists = zip(search_engine_list, revenue_list, keyword_list)
    answer_data = list(answer_lists)
    df_schema = StructType([StructField("search_engine", StringType()), StructField("revenue", StringType()), StructField("keyword", StringType())])
    answer_df = spark.createDataFrame(data=answer_data,schema=df_schema)
    result_df = answer_df.sort(answer_df.revenue.desc())
    return result_df

def write_tsv_file_to_s3(result_df):
    today = date.today()
    filename = str(today) + "_SearchKeywordPerformance.tsv"
    s3_client = boto3.client('s3')
    pandas_df = result_df.toPandas()
    csv_buffer = StringIO()
    pandas_df.to_csv(csv_buffer, sep="\t", index=False)
    s3_client.put_object(Body = csv_buffer.getvalue(),
                    Bucket='yavula-dev',
                    Key=filename)

def create_glue_database(db_name):

    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(db_name))
        logging.info("Database created as not existing already")
    except Exception as e:
        logging.error(e)
        logging.error("Unable to create the database. Exiting...")
        os._exit(1)

def write_sink_to_s3(raw_dynamic_frame):        
    sink = glueContext.getSink(connection_type="s3", path=TARGET_PATH, partitionKeys=["timeframe"], enableUpdateCatalog=True, updateBehavior="UPDATE_IN_DATABASE")
    sink.setFormat('glueparquet')
    sink.setCatalogInfo(catalogDatabase=TARGET_DATABASE, catalogTableName=TARGET_TABLE+"_snapshot")
    sink.writeFrame(raw_dynamic_frame)

def create_latest_snapshot_table(partition_value):
    
    if (spark._jsparkSession.catalog().tableExists(f"{TARGET_DATABASE}.{TARGET_TABLE}")):
        logging.info(
            "Updating the partition for the latest raw snapshot table...")
        spark.sql(
            f'ALTER TABLE {TARGET_DATABASE}.{TARGET_TABLE} SET LOCATION "s3://{TARGET_BUCKET}/{TARGET_PREFIX}/timeframe={partition_value}/"')
    else:
        logging.info(
            "Table pointing to the latest raw snapshot partition doesn't exist. Creating it...")
        spark.sql(
            f'CREATE TABLE {TARGET_DATABASE}.{TARGET_TABLE} USING PARQUET LOCATION "s3://{TARGET_BUCKET}/{TARGET_PREFIX}/timeframe={partition_value}/"')
    logging.info("The table has been updated.")

def main():

    tsv_data=read_input_file_from_s3(INPUT_PATH)
    order_ips,order_complete_df = select_purchased_ips(tsv_data)
    referrer_list = collect_referrer(order_ips,tsv_data)
    search_engine_list = extract_search_engine(referrer_list)
    keyword_list = extract_keyword(referrer_list)
    revenue_list = extract_revenue(order_complete_df)
    final_df = create_final_df(search_engine_list,revenue_list,keyword_list)
    timeframe= int(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
    timeframe_df = final_df.withColumn('timeframe', lit(timeframe))
    raw_dynamic_frame = DynamicFrame.fromDF(timeframe_df, glueContext, "raw_dynamic_frame")
    write_tsv_file_to_s3(final_df)
    create_glue_database(TARGET_DATABASE)
    write_sink_to_s3(raw_dynamic_frame)
    create_latest_snapshot_table(timeframe)
    
if __name__ == '__main__':
    main()
    