 imports
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
