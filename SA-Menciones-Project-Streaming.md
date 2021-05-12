<img src="https://qs.topuniversities.com/hs-fs/hubfs/07_HumanScience_H_300.jpg?width=4961&height=3508&name=07_HumanScience_H_300.jpg" width="250" height="100"  />

# Walk The Talk || Hands-On Big Data Streaming using Apache Spark 
## Real-Time Analysis of Your Political Standing
### Group F
#### Nicolas Fraire, Camille Eloi, Iñigo Hidalgo, Dea Markovic, Harshil Sharma, Oriol Vila, Gerardo Gandara


https://drive.google.com/file/d/1aMOA01zk4bPDQF32vH2UG2X44gDqfKZg/view?usp=sharing

One of the amazing frameworks that can handle big data in real-time and perform different analysis, is Apache Spark. I think there are many resources that provide information about different features of Spark and how popular it is in the big data community but shortly mentioning the core features of Spark: it does fast big data processing employing Resilient Distributed Datasets (RDDs), streaming and Machine learning on a large scale at real-time.


<img src="https://cdn.analyticsvidhya.com/wp-content/uploads/2018/07/performing-twitter-sentiment-analysis1.jpg" />
    



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

####  We are going to use findspark library to locate Spark on our local machine and then we import necessary packages from pyspark.


```python
#you need to put where is spark installed
# with this command : echo 'sc.getConfget('spark.home')' | spark-shell
findspark.init('/opt/spark-3.0.0-bin-hadoop3.2/')
```


```python
# May cause deprecation warnings, safe to ignore, they aren't errors
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc
```

#### We initiate SparkContext(). SparkContext is the entry point to any spark functionality. When we run any Spark application, a driver program starts, which has the main function and your SparkContext gets initiated here. A SparkContext represents the connection to a Spark cluster, and can be used to create RDDs, accumulators and broadcast variables on that cluster. SparkContext here uses Py4J to launch a JVM and creates a JavaSparkContext (source). 


```python
# Can only run this once. restart your kernel for any errors.
sc = SparkContext()
```

it is important to note that **only one SparkContext** may be running at each session. (That is why the trial and error is time consuming when you are learning, but it is ok, it is worthy!).

After that, we initiate the StreamingContext() with 10-second batch intervals, it means that the input streams will be divided into batches every 10 seconds during the streaming


```python
ssc = StreamingContext(sc, 10 )
sqlContext = SQLContext(sc)
```


```python
socket_stream = ssc.socketTextStream("127.0.0.1", 5555)

```


```python
ssc.checkpoint("checkpoint_TwitterApp")
```

#### Now, our next step is to assign our input source of streaming and then put the incoming data in variable lines

it is important to note that we are using the same port number (5555) as we used in the first module to send the tweets and the IP address (VM for our case) is the same since we are running things on our local machine. in addition

**We are using the window() function to determine that we are analyzing tweets every minute (60 seconds) to see what the top 10 #mentions are during that time**


```python
lines = socket_stream.window( 60 )
```

#### We will create TEMP tables for each candidate/mentions and a table with TWEETS for SA

### Politicians Count

### Then we need to calculate how many times the Candidate has been mentioned. We can do that by using the function reduceByKey. This function will calculate how many times the candidate has been mentioned per each batch, i.e. it will reset the counts in each batch. In this version for prototype purposes we used reduceByKey but it important to emphasize the difference. We can simulate it with a loop calculation later for you to see the difference.

#### In future cases, we need to calculate the counts across all the batches, so we’ll use another function called updateStateByKey, as this function allows you to maintain the state of RDD while updating it with new data. This way is called Stateful Transformation.


```python
# just a tuple to assign names
from collections import namedtuple

fields = ("candidato", "count" )
Candidato = namedtuple( 'Candidato', fields )

# here we apply different operations on the tweets and save them to #a temporary sql table

( lines.flatMap( lambda text: text.split( " " ) ) #Splits to a list
  # Checks for    candidate calls  
  .filter( lambda word: word.lower().startswith("ayuso") ) 
  .map( lambda word: ( word.lower(), 1 ) ) # Lower cases the word
  .reduceByKey( lambda a, b: a + b ) 
 # Stores in a Tweet Object
  .map( lambda rec: Candidato( rec[0], rec[1] ) )
 # Sorts Them in a dataframe
  .foreachRDD( lambda rdd: rdd.toDF().sort( desc("count") )
 # Registers only top 10 key words that starts with the candidate name to a table.
  .limit(10).registerTempTable("c_ayuso") ) )


```


```python
# just a tuple to assign names
from collections import namedtuple

fields = ("candidato", "count" )
Candidato = namedtuple( 'Candidato', fields )

# here we apply different operations on the tweets and save them to #a temporary sql table

( lines.flatMap( lambda text: text.split( " " ) ) #Splits to a list
  # Checks for    candidate calls  
  .filter( lambda word: word.lower().startswith("gabilondo") ) 
  .map( lambda word: ( word.lower(), 1 ) ) # Lower cases the word
  .reduceByKey( lambda a, b: a + b ) 
 # Stores in a Tweet Object
  .map( lambda rec: Candidato( rec[0], rec[1] ) )
 # Sorts Them in a dataframe
  .foreachRDD( lambda rdd: rdd.toDF().sort( desc("count") )
 # Registers only top 10 candidate to a table.
  .limit(10).registerTempTable("c_gabilondo") ) )


```


```python
# just a tuple to assign names
from collections import namedtuple

fields = ("candidato", "count" )
Candidato = namedtuple( 'Candidato', fields )

# here we apply different operations on the tweets and save them to #a temporary sql table

( lines.flatMap( lambda text: text.split( " " ) ) #Splits to a list
  # Checks for    candidate calls  
  .filter( lambda word: word.lower().startswith("monica") ) 
  .map( lambda word: ( word.lower(), 1 ) ) # Lower cases the word
  .reduceByKey( lambda a, b: a + b ) 
 # Stores in a Tweet Object
  .map( lambda rec: Candidato( rec[0], rec[1] ) )
 # Sorts Them in a dataframe
  .foreachRDD( lambda rdd: rdd.toDF().sort( desc("count") )
 # Registers only top 10 candidate to a table.
  .limit(10).registerTempTable("c_monica") ) )


```


```python
# just a tuple to assign names
from collections import namedtuple

fields = ("candidato", "count" )
Candidato = namedtuple( 'Candidato', fields )

# here we apply different operations on the tweets and save them to #a temporary sql table

( lines.flatMap( lambda text: text.split( " " ) ) #Splits to a list
  # Checks for    candidate calls  
  .filter( lambda word: word.lower().startswith("iglesias") ) 
  .map( lambda word: ( word.lower(), 1 ) ) # Lower cases the word
  .reduceByKey( lambda a, b: a + b ) 
 # Stores in a Tweet Object
  .map( lambda rec: Candidato( rec[0], rec[1] ) )
 # Sorts Them in a dataframe
  .foreachRDD( lambda rdd: rdd.toDF().sort( desc("count") )
 # Registers only top 10 candidate to a table.
  .limit(10).registerTempTable("c_iglesias") ) )


```


```python
# just a tuple to assign names
from collections import namedtuple

fields = ("candidato", "count" )
Candidato = namedtuple( 'Candidato', fields )

# here we apply different operations on the tweets and save them to #a temporary sql table

( lines.flatMap( lambda text: text.split( " " ) ) #Splits to a list
  # Checks for    candidate calls  
  .filter( lambda word: word.lower().startswith("edmundo") ) 
  .map( lambda word: ( word.lower(), 1 ) ) # Lower cases the word
  .reduceByKey( lambda a, b: a + b ) 
 # Stores in a Tweet Object
  .map( lambda rec: Candidato( rec[0], rec[1] ) )
 # Sorts Them in a dataframe
  .foreachRDD( lambda rdd: rdd.toDF().sort( desc("count") )
 # Registers only top 10 candidate to a table.
  .limit(10).registerTempTable("c_edmundo") ) )


```


```python
# just a tuple to assign names
from collections import namedtuple

fields = ("candidato", "count" )
Candidato = namedtuple( 'Candidato', fields )

# here we apply different operations on the tweets and save them to #a temporary sql table

( lines.flatMap( lambda text: text.split( " " ) ) #Splits to a list
  # Checks for    candidate calls  
  .filter( lambda word: word.lower().startswith("monasterio") ) 
  .map( lambda word: ( word.lower(), 1 ) ) # Lower cases the word
  .reduceByKey( lambda a, b: a + b ) 
 # Stores in a Tweet Object
  .map( lambda rec: Candidato( rec[0], rec[1] ) )
 # Sorts Them in a dataframe
  .foreachRDD( lambda rdd: rdd.toDF().sort( desc("count") )
 # Registers only top 10 candidate to a table.
  .limit(10).registerTempTable("c_monasterio") ) )


```


```python
#define the table structure
from collections import namedtuple
fields = ("tag", "SA" )
Tweet = namedtuple( 'Tweet', fields )
```


```python
(lines.map( lambda text: Tweet( text, "0" ) ) # Stores in a Tweet Object
    .foreachRDD( lambda rdd: rdd.toDF() # Sorts Them in a DF
    .limit(20).registerTempTable("tweets") ) # Registers to a table
) 
```


```python
import time
from IPython import display
#import seaborn as sns
import pandas

import re
from nltk.corpus import stopwords
```


```python
# to clean tweets for the SA
import nltk
#nltk.download('stopwords')
```

### Run the TweetRead.py file at this point (in the other terminal windows)

Now, we can run receive-Tweets.py and after that we can start streaming by running : ssc.start()


```python
ssc.start()
```

# Google API Sheets


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

# -----  Mentions ------


```python
sqlContext.sql("select * from c_ayuso").show()
df_cont = sqlContext.sql("select * from c_ayuso")
df_ay = df_cont.toPandas()
```

    +---------+-----+
    |candidato|count|
    +---------+-----+
    |    ayuso|    4|
    |   ayuso.|    2|
    |   ayuso,|    1|
    +---------+-----+
    



```python
sqlContext.sql("select * from c_iglesias").show()
df_cont = sqlContext.sql("select * from c_iglesias")
df_ig = df_cont.toPandas()
```

    +------------+-----+
    |   candidato|count|
    +------------+-----+
    |    iglesias|    5|
    | iglesias"rt|    1|
    |iglesias.…rt|    1|
    |   iglesias,|    1|
    +------------+-----+
    



```python
sqlContext.sql("select * from c_monasterio").show()
df_cont = sqlContext.sql("select * from c_monasterio")
df_ms = df_cont.toPandas()
```

    +------------+-----+
    |   candidato|count|
    +------------+-----+
    |monasterio',|    1|
    +------------+-----+
    


### For now we will do only the 3 populars


```python
#sqlContext.sql("select * from c_edmundo").show()
#df_cont = sqlContext.sql("select * from c_edmundo")
#df_ed = df_cont.toPandas()
```


```python
#sqlContext.sql("select * from c_monica").show()
#df_cont = sqlContext.sql("select * from c_monica")
#df_mo = df_cont.toPandas()
```


```python
#sqlContext.sql("select * from c_gabilondo").show()
#df_cont = sqlContext.sql("select * from c_gabilondo")
#df_ga = df_cont.toPandas()
```


```python
# append all the 6 DF
#df_menciones = pd.concat([df_ay, df_ig, df_ga, df_mo, df_ms, df_ed]).drop_duplicates(subset=['candidato'])
```

we can append the df and collect in one candidate


```python
# append all the 3 DF
df_menciones = pd.concat([df_ay, df_ig, df_ms]).drop_duplicates(subset=['candidato'])
```


```python
df_menciones.candidato = df_menciones.candidato.apply(lambda x: 'ISABEL DIAZ AYUSO' if 'ayuso' in x else x)
df_menciones.candidato = df_menciones.candidato.apply(lambda x: 'PABLO IGLESIAS' if 'iglesias' in x else x)
df_menciones.candidato = df_menciones.candidato.apply(lambda x: 'ROCIO MONASTERIO' if 'monasterio' in x else x)
```


```python
df_menciones
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>candidato</th>
      <th>count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>ISABEL DIAZ AYUSO</td>
      <td>4</td>
    </tr>
    <tr>
      <th>1</th>
      <td>ISABEL DIAZ AYUSO</td>
      <td>2</td>
    </tr>
    <tr>
      <th>2</th>
      <td>ISABEL DIAZ AYUSO</td>
      <td>1</td>
    </tr>
    <tr>
      <th>0</th>
      <td>PABLO IGLESIAS</td>
      <td>5</td>
    </tr>
    <tr>
      <th>1</th>
      <td>PABLO IGLESIAS</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2</th>
      <td>PABLO IGLESIAS</td>
      <td>1</td>
    </tr>
    <tr>
      <th>3</th>
      <td>PABLO IGLESIAS</td>
      <td>1</td>
    </tr>
    <tr>
      <th>0</th>
      <td>ROCIO MONASTERIO</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>



# Mentions to Google Sheet LIVE!


```python
# get the instance of the Spreadsheet
sheet = client.open('STREAMING-MENTIONS')
```


```python
count = 0
df_ant = df_menciones

#insert the first query
sheet_instance = sheet.get_worksheet(0)
sheet_instance.insert_rows(df_menciones.values.tolist(), row = 2)

while count < 5:
    print("Lote -----> " , count + 1)
    time.sleep( 3 )
    
    ##-----------------Create DF_Mentions----------------------##
    df_cont = sqlContext.sql("select * from c_ayuso")
    df_ay = df_cont.toPandas()

    df_cont = sqlContext.sql("select * from c_monasterio")
    df_ms = df_cont.toPandas()

    df_cont = sqlContext.sql("select * from c_iglesias")
    df_ig = df_cont.toPandas()

    # append all the 3 DF
    df_menciones = pd.concat([df_ay, df_ig, df_ms]).drop_duplicates(subset=['candidato'])

    df_menciones.candidato = df_menciones.candidato.apply(lambda x: 'ISABEL DIAZ AYUSO' if 'ayuso' in x else x)
    df_menciones.candidato = df_menciones.candidato.apply(lambda x: 'PABLO IGLESIAS' if 'iglesias' in x else x)
    df_menciones.candidato = df_menciones.candidato.apply(lambda x: 'ROCIO MONASTERIO' if 'monasterio' in x else x)
    #-----------------END Create DF_Mentions----------------------##
    
    
    if not(df_ant.equals(df_menciones)):
        # get the first sheet of the Spreadsheet (SA)
        sheet_instance = sheet.get_worksheet(0)
        sheet_instance.insert_rows(df_menciones.values.tolist(), row = 2)
    
    df_ant = df_menciones
    count = count + 1

```

    Lote ----->  1
    Lote ----->  2
    Lote ----->  3
    Lote ----->  4
    Lote ----->  5


# ---- SA ---- Twitts


```python
# get the instance of the Spreadsheet
sheet = client.open('SA-Candidates')
```


```python
sqlContext.sql("select * from tweets").show()

```

    +--------------------+---+
    |                 tag| SA|
    +--------------------+---+
    |Por…@PabloEcheniq...|  0|
    |#GobiernoDimision...|  0|
    |No saben q…@elmun...|  0|
    |A ver si esto les...|  0|
    |                    |  0|
    |Defienden modific...|  0|
    |                    |  0|
    |Por…RT @Tonicanto...|  0|
    |                    |  0|
    |Por…RT @Accountab...|  0|
    |                    |  0|
    |-Cloacas contra P...|  0|
    |-Lezo y otras cor...|  0|
    |  -Cohecho con Ayuso|  0|
    |-Manipulación co…...|  0|
    |Verg… https://t.c...|  0|
    |"SÍ SE PUEDE. QUE...|  0|
    +--------------------+---+
    


#### Let's define a procedure to clean the tweet


```python
def process_text(raw_text):
    letters_only = re.sub("[^A-zÀ-ú]", " ",str(raw_text)) 
    #print(letters_only)
    letters_only = re.sub("([HhJj][A-zÀ-ú]){2,}[HhJj]*", "jajaja", str(letters_only))
    words = letters_only.lower().split()
    #print(words)
    stops = set(stopwords.words("spanish"))  
    not_stop_words = [w for w in words if not w in stops]
    return (" ".join(not_stop_words))
```

### We can enter in a loop and it will be calculating the SA real-time, getting the tweets from the TABLE SQL 
that is calculated/produced above - Let's start with 20 - it can be infinite and the dashboard gets constant updated


```python
df1_content = sqlContext.sql( 'Select tag, SA from tweets' )
df1 = df1_content.toPandas()

    
count = 0

while count < 1:
    print("Lote -----> " , count + 1)
    time.sleep( 3 )
    df2_content = sqlContext.sql( 'Select tag, SA from tweets' )
    df2 = df2_content.toPandas()
    
    # Concatenating dataframes without duplicates
    df_tweets = pd.concat([df1, df2]).drop_duplicates(subset=['tag'])
    df_tweets = df_tweets[df_tweets['tag'].apply(len)>10]

    df_tweets['SA'] = ""
    i = 0
    
    for i, row in df_tweets.iterrows():
        print(" * Tweet *  - " , df_tweets["tag"][i])
        twit = df_tweets["tag"][i]
        twit = process_text(twit)
        sa = analyzer.predict(twit)
        df_tweets.at[i,'SA'] = sa
        print("The sentiment ---> ", sa, "\n")
    
    df=df_tweets.copy()
    sheet_instance = sheet.get_worksheet(0)
    sheet_instance.insert_rows(df.values.tolist(), row = 2)
    count = count + 1

    # get the first sheet of the Spreadsheet (SA)


    df1 = df_tweets.copy()
    df1['SA'] = "0"
```

    Lote ----->  1
     * Tweet *  -  Por…@PabloEchenique Deberías preguntarte cuántos Podemonguers habría disfrutando del botellón después de haber criticad… https://t.co/1ekZJCBVYXRT @Tonicanto1: El socialismo es que Begoña Gómez -que dirige una cátedra extraordinaria de Transformación Social, sin ser catedrática ni t…RT @cincinatoss: Saben lo que viene, como Pablo Iglesias, y huyen de la quema. 
    The sentiment --->  NEG 
    
     * Tweet *  -  #GobiernoDimisionRT @Cazatalentos: La cara de orgasmo de Ferreras y compañía con la renuncia de Pablo Iglesias se les hacía parecer muy felices. 
    The sentiment --->  NEG 
    
     * Tweet *  -  No saben q…@elmundoes Entiendo q como además Biden está copiando sus “ políticas” en USA, en breves tb culparán a Ayuso del asesinato de KennedyRT @cat_nordic: (4/5) A Madrid, però, la ultra-dreta està desbocada i per això la judicatura li va fotre enlaire ahir, al “guapito de cara”…RT @LeonelFernandez: #MartesDeLectura: ¿Es correcto calificar de populista a figuras tan disímiles como Hugo Chávez, Donald Trump, Marine L…RT @romaochaita: Como diría el gran Eugenio.. Se saben aquel que diu... un pobre menesteroso en Madrid encontrase una lampara maravillosa y…RT @PSOE: Por fin han tenido una victoria electoral...bueno, ustedes no, Ayuso.
    The sentiment --->  NEG 
    
     * Tweet *  -  A ver si esto les tranquiliza...
    The sentiment --->  NEU 
    
     * Tweet *  -  Defienden modificaciones…El Gobierno afirma que los malos datos covid de Madrid han perjudicado a España ante el Reino Unido https://t.co/ErMuxsUvGPRT @And89_3: Pablo Iglesias peleó la subida del SMI, el IMV o la subida de las pensiones y tú le odias, Ana Botella vendió viviendas públic…RT @Jakimuca: ¿Por qué Ana Rosa Quintana arremete contra Podemos y Pablo Iglesias? https://t.co/BDUzNDgQuNRT @Tonicanto1: El socialismo es que Begoña Gómez -que dirige una cátedra extraordinaria de Transformación Social, sin ser catedrática ni t…Ayuso contra la OCDE, contra la decencia y contra la población en general. Eso sí, defendiendo al 1% más rico con t… https://t.co/V241cS1K7lRT @pablocast13: Dice la ministra de Exteriores que la culpa de que Reino Unido recomiende no viajar a España este verano es de Ayuso.
    The sentiment --->  NEG 
    
     * Tweet *  -  Por…RT @Tonicanto1: El socialismo es que Begoña Gómez -que dirige una cátedra extraordinaria de Transformación Social, sin ser catedrática ni t…RT @AntonioRNaranjo: Tienen que ver el manojo de sopapos a mano abierta que esta periodista americana da a Pedro Sánchez y Pablo Iglesias.…RT @pablocast13: Dice la ministra de Exteriores que la culpa de que Reino Unido recomiende no viajar a España este verano es de Ayuso.
    The sentiment --->  NEG 
    
     * Tweet *  -  Por…RT @Accountable2019: Atresmedia:
    The sentiment --->  NEU 
    
     * Tweet *  -  -Cloacas contra Podemos e independentistas
    The sentiment --->  NEU 
    
     * Tweet *  -  -Lezo y otras corrupciones
    The sentiment --->  NEG 
    
     * Tweet *  -  -Cohecho con Ayuso
    The sentiment --->  NEG 
    
     * Tweet *  -  -Manipulación co…@ACOM_es @Tonicanto1 GONZÁLEZ LAYA: "LA IRRESPONSABILIDAD DE MADRID HA PERJUDICADO A ESPAÑA EN EL REINO UNIDO"
    The sentiment --->  NEG 
    
     * Tweet *  -  Verg… https://t.co/hrr0mwadFIRT @ProLibertate19: "Ayuso como exemplo para a salvação de Sánchez" por @pedromdsa1 https://t.co/of8HY3PBclRT @pedroagbilbao: Atentos: hace unos tuíts expuse el temor de que PPSOE maniobran para quitarse socios, restablecer el bipartidismo inclus…@romaochaita Ayuso hace mucho daño con "su" libertad (libertinaje) a los sanitarios de toda España. Un saludo afectuoso. Sé felizRT @gabriellaperu: Pablo Iglesias no viene a apoyar a Castillo, sino a promover las movilizaciones, incendios, tomas de carretera y desmane…RT @pablom_m: Solo vengo para decir que la fiscalía anticorrupción asegura que Rodrigo Rato ocultó 77 millones de euros en una sociedad de…RT @TabarniaB: Este fue el último tuit de Pablo Iglesias, hace 8 días. 
    The sentiment --->  NEG 
    
     * Tweet *  -  "SÍ SE PUEDE. QUE HABLE LA MAYORÍA" decía... 
    The sentiment --->  NEU 
    


## Inside the loop we have the DF (df_tweets) that is being calculated.
## At the end of the loop, we will have the last DF with the SA to be published in Tablaeu
## This will be refreshed everytime that we run the LOOP


```python
#df=df_tweets.copy()
#sheet_instance = sheet.get_worksheet(0)
#sheet_instance.insert_rows(df.values.tolist(), row = 2)
```


```python
df_tweets = df_tweets.sort_values(by=['tag'])
df_tweets
```


```python
ssc.stop()
```

# Thanks!


```python

```
