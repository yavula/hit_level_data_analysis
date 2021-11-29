# Hit level data analysis
## Introduction
The task is to analyze a provided 'hit level' data file and provide the client information regarding the performance of various search engines and the key words which redirect the customers to the 'Esshopzilla' site.
## Contents of this repository
- There is a AWS Glue pyspark script `glue_script.py`, the output `.tsv` file taken from AWS s3 and a business presentation to a potential client.
## glue_script.py
The python script to execute the application in AWS Glue.
### Description
- The `read_input_file_from_s3` function takes the tsv data file from s3 and reads it as a spark dataframe. 
- The data for which the order is completed is taken into a dataframe `order_complete_df` as this is the data which produces revenue for the client. The orders whose `pagename` does not have `Order Complete` are not considered for this analysis because the main business questions here are to find out which external search engine provides highest revenue and which keywords are performing best based on revenue.
- The function `select_purchased_ips` filters the ip's whose order is complete.
- `collect_referrer` function makes a list out of the referrer entries which have their orders completed.
- The function `extract_search_engine` extracts the search engine type from the `referrer` column and adds it to a search_engine_list. Similary the functions `extract_keyword`, `extract_revenue` extract the keywords and revenue and return the lists as keyword_list, revenue_list respectively.
- `create_final_df` zips the lists created above so that each element in the row is paired together. They are stored as a dataframe after sorting based on revenue.
- `write_tsv_file_to_s3` writes the dataframe as tsv file to s3 using boto3.
- `create_glue_database` checks if the required database is existing in glue and creates one if not existing. `write_sink_to_s3` writes the spark dataframe as dynamic frame to s3 and maps the data to the table in AWS Glue. 
