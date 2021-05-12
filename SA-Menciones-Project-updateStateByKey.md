## Google API Sheets - Configuration


```python
#!pip3 install google-api-python-client==1.6.7
```


```python
#!pip3 install gspread
```


```python
#!pip3 install oauth2client 
```


```python
# importing the required libraries
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
```


```python
# define the scope
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']

# add credentials to the account
creds = ServiceAccountCredentials.from_json_keyfile_name('madridelections2021.json', scope)

# authorize the clientsheet 
client = gspread.authorize(creds)

```

#### Install Library for SA


```python
#!pip3 install --upgrade pip
#!pip3 install pysentimiento
#!pip3 install torch
#!pip3 install sklearn
```


```python
from pysentimiento import SentimentAnalyzer
analyzer = SentimentAnalyzer()
```


```python
import findspark
import pandas as pd
```

#### Spark Configuration


```python
#you need to put where is spark installed
# with this command : echo 'sc.getConfget('spark.home')' | spark-shell
findspark.init('/opt/spark-3.0.0-bin-hadoop3.2/')
```


```python
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")

# create spark context with the above configuration - Can only run this once. restart your kernel for any errors.
sc = SparkContext(conf=conf)

sc.setLogLevel("ERROR")
# create the Streaming Context from the above spark context with interval size 2 seconds
ssc = StreamingContext(sc, 10)
# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")

#lines = socket_stream.window( 60 )
# read data from port 5555
dataStream = ssc.socketTextStream("127.0.0.1",5555)

```

#### Politician Count Mentions


```python
def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)
```


```python
def send_df_to_dashboard(df):
    import json  
    # extract the mentioned candidate from dataframe and convert them into array
    top_mentioned = [str(t.mentioned) for t in df.select("mentioned").collect()]
    
    # extract the counts from dataframe and convert them into array
    mentions_count = [p.mentions_count for p in df.select("mentions_count").collect()]   
    
    # initialize and send the data through REST API
    url = 'https://sheet.best/api/sheets/c32f21e3-f404-490a-bce6-a7b90f2903f3'    
    
    #In Python works
    #json_string = df.to_json(orient="records")
    #json_obj = json.loads(json_string)
    #response = requests.post(url, json=json_obj)
    
    #In PySpark we have the same toJASON = df.toJSON().collect()
 
    #but I need to transform it, in Python works, but in PySpark doesnt.
    
    temp1 = temp2.toJSON.collect()
    print("temp1", temp1)
    
    #Tried with dumps and loads but no luck.....
    print ("-----dumps", json.dumps(temp2) )
    
    #The rest API accepts JSON but it comes with an extra "  '  " in spark...dont know why     
          
    #sends to google REST API
    request_data = {'mentioned': str(top_mentioned), 'mentions_count': str(mentions_count)}
    #response = requests.post(url, data=request_data)

    print("con duplets", request_data)
        
    response = requests.post(url, data=request_data)    
    print(response)
```


```python
def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']


def process_rdd(time, rdd):
    print("----------- %s -----------" % str(time))
    try:
        # Get spark sql singleton context from the current context
        sql_context = get_sql_context_instance(rdd.context)
        # convert the RDD to Row RDD
        row_rdd = rdd.map(lambda w: Row(mentioned=w[0], mentions_count=w[1]))
        # create a DF from the Row RDD
        mentions_df = sql_context.createDataFrame(row_rdd)
        # Register the dataframe as table
        mentions_df.registerTempTable("mentions")
        # get the top 10 mentions from the table using SQL and print them
        mentioned_counts_df = sql_context.sql("select mentioned, mentions_count from mentions order by mentions_count desc limit 10")
        mentioned_counts_df.show()
        # call this method to prepare top 10 mentioned candidates DF and send them
        send_df_to_dashboard(mentioned_counts_df)    
    except:
        e = sys.exc_info()[0]
        print("Warning - No mentions : %s" % e)
```


```python

# get the instance of the Spreadsheet
#sheet = client.open('STREAMING-MENTIONS')


# split each tweet into words
words = dataStream.flatMap(lambda line: line.split(" "))
# filter the words to get only mentioned candidate, then map each mentioned to be a pair of (mentioned,1)

mentioned = words.filter( lambda w: w.lower().startswith( ("ayuso", "iglesias", "monasterio", "gabilondo", "edmundo", "monica"))).map(lambda x: (x, 1) )
#mentioned = words.filter( lambda w: 'ayuso' in w).map(lambda x: (x, 1))

                         # adding the count of each mentioned to its last count
tags_totals = mentioned.updateStateByKey(aggregate_tags_count)
                         
# do processing for each RDD generated in each interval
tags_totals.foreachRDD(process_rdd)

# start the streaming computation
ssc.start()




# wait for the streaming to finish
ssc.awaitTermination()
```

    ----------- 2021-05-12 09:29:40 -----------
    Warning - No mentions : <class 'ValueError'>
    ----------- 2021-05-12 09:29:50 -----------
    Warning - No mentions : <class 'ValueError'>
    ----------- 2021-05-12 09:30:00 -----------
    +---------+--------------+
    |mentioned|mentions_count|
    +---------+--------------+
    |    Ayuso|             3|
    |    AYUSO|             1|
    +---------+--------------+
    
    temp2 <bound method DataFrame.toJSON of DataFrame[mentioned: string, mentions_count: bigint]>
    Warning - No mentions : <class 'AttributeError'>
    ----------- 2021-05-12 09:30:10 -----------
    +---------+--------------+
    |mentioned|mentions_count|
    +---------+--------------+
    |    Ayuso|             3|
    | Iglesias|             2|
    |   Ayuso,|             1|
    |    AYUSO|             1|
    +---------+--------------+
    
    temp2 <bound method DataFrame.toJSON of DataFrame[mentioned: string, mentions_count: bigint]>
    Warning - No mentions : <class 'AttributeError'>
    ----------- 2021-05-12 09:30:20 -----------



    ---------------------------------------------------------------------------

    KeyboardInterrupt                         Traceback (most recent call last)

    <ipython-input-14-a914c170a9fb> in <module>
         23 
         24 # wait for the streaming to finish
    ---> 25 ssc.awaitTermination()
    

    /opt/spark-3.0.0-bin-hadoop3.2/python/pyspark/streaming/context.py in awaitTermination(self, timeout)
        187         """
        188         if timeout is None:
    --> 189             self._jssc.awaitTermination()
        190         else:
        191             self._jssc.awaitTerminationOrTimeout(int(timeout * 1000))


    /opt/spark-3.0.0-bin-hadoop3.2/python/lib/py4j-0.10.9-src.zip/py4j/java_gateway.py in __call__(self, *args)
       1301             proto.END_COMMAND_PART
       1302 
    -> 1303         answer = self.gateway_client.send_command(command)
       1304         return_value = get_return_value(
       1305             answer, self.gateway_client, self.target_id, self.name)


    /opt/spark-3.0.0-bin-hadoop3.2/python/lib/py4j-0.10.9-src.zip/py4j/java_gateway.py in send_command(self, command, retry, binary)
       1031         connection = self._get_connection()
       1032         try:
    -> 1033             response = connection.send_command(command)
       1034             if binary:
       1035                 return response, self._create_connection_guard(connection)


    /opt/spark-3.0.0-bin-hadoop3.2/python/lib/py4j-0.10.9-src.zip/py4j/java_gateway.py in send_command(self, command)
       1198 
       1199         try:
    -> 1200             answer = smart_decode(self.stream.readline()[:-1])
       1201             logger.debug("Answer received: {0}".format(answer))
       1202             if answer.startswith(proto.RETURN_MESSAGE):


    /usr/lib/python3.7/socket.py in readinto(self, b)
        587         while True:
        588             try:
    --> 589                 return self._sock.recv_into(b)
        590             except timeout:
        591                 self._timeout_occurred = True


    KeyboardInterrupt: 



```python
ssc.stop()
```


```python

```
