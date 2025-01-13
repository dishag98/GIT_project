from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import pytz


def create_my_sql_data_frame(dbname, table_name, environment, spark):
    """create spark dataframe from mysql"""
    data_frame = None
    mysql_hostname = 'db.pre-prod.flipkart.com'
    mysql_jdbcPort = 3306
    mysql_dbname = dbname
    mysql_username = 'root'
    mysql_password = 'my_pass'

    if environment == 'prod':
        mysql_hostname = 'db.prod.flipkart.com'
        mysql_jdbcPort = 3306
        mysql_dbname = dbname
        mysql_username = 'dev'
        mysql_password = 'pass_2'

    mysql_connectionProperties = {
        "user": mysql_username,
        "password": mysql_password,
        "driver": "com.mysql.jdbc.Driver"
    }

    mysql_jdbc_url = "jdbc:mysql://{0}:{1}/{2}?DateTimeBehavior".format(mysql_hostname,
                                                                                          mysql_jdbcPort,
                                                                                          mysql_dbname)
    data_frame = spark.read.jdbc(url=mysql_jdbc_url, table=table_name, properties=mysql_connectionProperties)

    return data_frame
def get_rds_to_s3_transformed_data(emp , df):
    df = df.select(
        df.empid,
        df.fname,
        df.lname,
        df.loc,
        df.mobNo
    )
# transformation

    df_cust = df.select(
                                df.empid,
                                df.fname,
                                df.lname,
                                df.loc,
                                df.mobNo
                            )
    order_join = df_cust.join(emp, df_cust.empid == emp.cust_id, how='inner')

    df_final = order_join.select(
        df_cust.empid,
        df_cust.fname,
        df_cust.lname,
        df_cust.loc.alias('emp_loc'),
        df_cust.mobNo.alias('emp_mobNo'),
        df_cust.updated_date,
        df_cust.modified_date,
        emp.order_id,
        emp.product_name,
        emp.loc.alias('order_loc'),
        emp.order_mob_no
    )
    return order_join
def main():



    # it represents the current date
    local_tz = pytz.timezone('Asia/Kolkata')
    date_key = datetime.now(local_tz)

    # raw data
    df_cust = create_my_sql_data_frame('flipcart', 'customer', 'prod', spark)
    df_orders = create_my_sql_data_frame('flipcart', 'order', 'prod', spark)


    df_cust.write.mode('overwrite').parquet(
        f"s3://flipkart-data-engg-data-prod/transformed/sells_data/{date_key}")


if __name__ == "__main__":
    main()
