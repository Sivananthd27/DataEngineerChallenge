import glob
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import split
from pyspark.sql.types import *

#building Spark Session
spark = SparkSession.builder \
    .master("local") \
    .appName("Sessionize") \
    .config("spark.executor.memory", "1gb") \
    .getOrCreate()
sc = spark.sparkContext
spark = SparkSession(sc)

#reading the log file
raw_data_files = glob.glob(r'C:\Users\User\Desktop\DataTesting\DataEngineerChallenge\data\2015_07_22_mktplace_shop_web_log_sample.log.gz')
log_df = spark.read.text(raw_data_files)

#checking the schema
log_df.printSchema()

#checking sample data in dataframe
log_df.show(10, truncate=False)

#Checking row count to match after converting to multiple columns
print((log_df.count(), len(log_df.columns)))

#Checking null value
print(log_df.filter(log_df['value'].isNull()).count())

#Converting single column in dataframe to multiple columns based on the schema
split_col = split(log_df['value'], ' ')
ip_pattern = r'(^\S+\.[\S+\.]+\S+)\s'
split_log_df = log_df.withColumn('logtimestamp', split_col.getItem(0).cast(TimestampType())) \
            .withColumn('clientipport', (split_col.getItem(2))) \
            .withColumn('useragent', split_col.getItem(12))
split_ip = split(split_log_df['clientipport'], ':')
split_log_df = split_log_df.withColumn('clientip', split_ip.getItem(0))

# sessionizing data based on 15 min fixed window time and counting the hits
# assigning an uniqeId to each session grouped by ip and count of hits
sessionized_df = split_log_df.select(window("logtimestamp", "15 minutes").alias('timewindow'),'logtimestamp','clientip')\
                 .groupBy('timewindow','clientip').count().withColumnRenamed('count', 'numofpagehits')\
                 .orderBy("numofpagehits")\
                 .withColumn("sessionid", monotonically_increasing_id())
sessionized_df.show(20,False)

# adding timestamp and url to the session dataframe for finding the first hit time
timeurldf = split_log_df.select(window("logtimestamp", "15 minutes").alias('timewindow'),'logtimestamp',"clientip","useragent")
sessionized_df = timeurldf.join(sessionized_df,['timewindow','clientip'])
sessionized_df.show(10,False)

# Finding the first hit timestamp for each ip for each session
firsthittime = sessionized_df.groupBy("sessionid").agg(min("logtimestamp").alias('fristhittime'))
sessionized_df = firsthittime.join(sessionized_df,['sessionid'])
sessionized_df.select(col("sessionid"),col("clientip"),col("fristhittime")).show(20)

# Average session time
# the time difference between first and last hit in a session is the duration of a session for each IP
# if there is only one hit in a session the duration is zero
sessiontimediff = (unix_timestamp(sessionized_df.logtimestamp)-unix_timestamp(sessionized_df.fristhittime))
sessionized_df = sessionized_df.withColumn("timediff", sessiontimediff)
tmpdf = sessionized_df.groupBy("sessionid").agg(max("timediff").alias("sessioninterval"))
sessionized_df = sessionized_df.join(tmpdf,['sessionid'])
sessionized_df.select(col("sessionid"),col("clientip"),col("sessioninterval")).show(20)
averagedf = sessionized_df.groupBy().avg('sessioninterval')
averagedf.show()

#URL visits per session. unique URL count once per session
sessiondfurl = sessionized_df.groupBy("sessionid","useragent").count().distinct().withColumnRenamed('count', 'urlhitcount')
sessiondfurl.show(20)

#most engaged users, IPs with the longest session times
mostengageduser = sessionized_df.select("clientip","sessionid","sessioninterval").sort(col("sessioninterval").desc()).distinct()
mostengageduser.show(20)
