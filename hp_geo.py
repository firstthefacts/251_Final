from pyspark.sql import SparkSession
from pyspark.sql import SQLContext, Row
import json
import re
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
def add_geo_info(json_str):
    GEOIP = pygeoip.GeoIP("/opt/GeoLiteCity.dat", pygeoip.MEMORY_CACHE)
    try:
        geo_dict = GEOIP.record_by_addr(json_str["source_ip"])
        json_str["country"] = geo_dict["country_name"]
	json_str["ctry_code"]=geo_dict["country_code"]
        json_str["city"] = geo_dict["city"]
        json_str["region"] = geo_dict["region_code"]
        json_str["dma"] = geo_dict["dma_code"]
    except:
        country = ""
        city = ""
        region = ""
        dma = ""
 
    return json_str 

# The entry point into all functionality in Spark is the SparkSession class. 
# To create a basic SparkSession, just use SparkSession.builder:

spark = SparkSession \
    .builder \
    .appName("HP_spark_processor") \
    .getOrCreate()

# spark context
sc = spark.sparkContext

# read in the attack data from hdfs
attack_data=sc.textFile("hdfs:/user/root/honey4_out.json/part-00000-cde36a09-a4c0-4730-87d6-60def9df8239.json")

# map each attack to a json string
attack_json=attack_data.map(lambda x : json_loads(x))

# add the geo-lookup values to the json string
geo_json = attack_json.map(lambda x : add_geo_info(x))

jsonDF = geo_json.toDF()

jsonDF.show()

# +----+-------+
# | age|   name|
# +----+-------+
# |null|Michael|
# |  30|   Andy|
# |  19| Justin|
# +----+-------+

jsonDF.printSchema()
