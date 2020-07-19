import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import java.time.Instant


object Sessionize {
  //test code ignore
  def testRun()= {
    val line = """2015-07-22T09:00:28.019143Z elb-shop"""
    val p = new ElbParser
    val r = p.analyze2(line)
    println(r)
  }

  //Get a spark session and context
  def getSparkSession(name:String):SparkSession={
    val spark = SparkSession.builder().appName(name)
      .master(master="local[4]")
      .config("spark.sql.caseSensitive", "true")
      .enableHiveSupport()
      .getOrCreate()
    return spark
  }

  //load ELB data and get it as a Dataframe
  def loadDf():DataFrame={
    println("Start loading..")
    val spark = getSparkSession("Session_App")
    import spark.implicits._
    val parser = new ElbParser
    //Map Elbparser onto RDD
    val logRdd = spark.sparkContext.textFile("src/main/resources/elb.log").filter(parser.elbFilter(_)).map(parser.parseElb(_))
    val df = logRdd.map({
      //I guess there is a better short hand to this.
      case Row(val1:String,val2:String,val3:String,val4:String,val5:String,val6:String,val7:String,val8:String,val9:String,val10:String,val11:String,val12:String,val13:String,val14:String,val15:String,val16:String,val17:String)=>ElbRow(val1,val2,val3,val4,val5,val6,val7,val8,val9,val10,val11,val12,val13,val14,val15,val16,val17)
    }).toDF()
    //print DF for confirmation
    //df.show(10)
    //returns are un-needed in scala but makes the code clearer for readers of other languages
    return df
  }

  //Sessionize and Analyze
  def analyzeSession()={
    val ipGroupWindow = Window.partitionBy("client_ip").orderBy("time")
    val previousTs = lag("time",1).over(ipGroupWindow)
    //Assuming any break in activity >15min ie 900secs is a new session
    val ssn = when((col("diff_ses").geq(900) || col("epoch_prev_ts").equals(-1)), 1).otherwise(0)
    val df = loadDf()
    val newDf = df.withColumn("previous_ts",previousTs)
    val udfEpoch = udf(getEpoch)
    val udfUrl = udf(parseUrlFromRequest)
    val newdf2 = newDf
      .withColumn("epoch_ts",udfEpoch(col("time")))
      .withColumn("epoch_prev_ts", udfEpoch(col("previous_ts")))
      .withColumn("diff_ses", col("epoch_ts")-col("epoch_prev_ts"))
      .withColumn("session",ssn)
      .withColumn("url",udfUrl(col("request")))

    val sessionDf = newdf2.select("time","client_ip","previous_ts","epoch_ts","epoch_prev_ts","diff_ses","session","url").orderBy(asc("client_ip"),asc("time"))
    //sessionDf.show(100,false)
    val userSessionWindow = Window.partitionBy("client_ip").orderBy("time")
    val sumUserSession = sum("session").over(userSessionWindow)
    val sessionDf2 = sessionDf.withColumn("user_session_id",sumUserSession)
    //This is a sessioniozed dataframe now where user_session_id shows a unique session id
    //sessionDf2.orderBy(asc("client_ip"),asc("time")).show(100,false)
    //      +---------------------------+-------------+---------------------------+----------+-------------+----------+-------+-----------------------------------------------------------------------------------------------------------------------------------------------+---------------+
    //      |time                       |client_ip    |previous_ts                |epoch_ts  |epoch_prev_ts|diff_ses  |session|url                                                                                                                                            |user_session_id|
    //      +---------------------------+-------------+---------------------------+----------+-------------+----------+-------+-----------------------------------------------------------------------------------------------------------------------------------------------+---------------+
    //      |2015-07-22T10:45:55.881199Z|1.186.101.79 |null                       |1437561955|-1           |1437561956|1      |https://paytm.com:443/shop/wallet/balance                                                                                                      |1              |
    //      |2015-07-22T10:45:55.885488Z|1.186.101.79 |2015-07-22T10:45:55.881199Z|1437561955|1437561955   |0         |0      |https://paytm.com:443/shop/wallet/txnhistory                                                                                                   |1              |
    //      |2015-07-22T10:46:27.839734Z|1.186.101.79 |2015-07-22T10:45:55.885488Z|1437561987|1437561955   |32        |0      |https://paytm.com:443/shop/v1/frequentorders                                                                                                   |1              |
    //      |2015-07-22T10:46:56.591943Z|1.186.101.79 |2015-07-22T10:46:27.839734Z|1437562016|1437561987   |29        |0      |https://paytm.com:443/papi/v1/expresscart/verify                                                                                               |1              |
    //      |2015-07-22T10:47:01.782695Z|1.186.101.79 |2015-07-22T10:46:56.591943Z|1437562021|1437562016   |5         |0      |https://paytm.com:443/api/v1/expresscart/checkout                                                                                              |1              |
    //      |2015-07-22T10:47:06.893987Z|1.186.101.79 |2015-07-22T10:47:01.782695Z|1437562026|1437562021   |5         |0      |https://paytm.com:443/shop/summary/1116587591                                                                                                  |1              |
    //      |2015-07-22T10:47:07.616869Z|1.186.101.79 |2015-07-22T10:47:06.893987Z|1437562027|1437562026   |1         |0      |https://paytm.com:443/shop/cart                                                                                                                |1              |
    //      |2015-07-22T10:47:07.844446Z|1.186.101.79 |2015-07-22T10:47:07.616869Z|1437562027|1437562027   |0         |0      |https://paytm.com:443/shop/orderdetail/1116587591                                                                                              |1              |
    //      |2015-07-22T10:47:18.072370Z|1.186.101.79 |2015-07-22T10:47:07.844446Z|1437562038|1437562027   |11        |0      |https://paytm.com:443/shop/orderdetail/1116587591                                                                                              |1              |
    //      |2015-07-22T10:47:28.084661Z|1.186.101.79 |2015-07-22T10:47:18.072370Z|1437562048|1437562038   |10        |0      |https://paytm.com:443/shop/orderdetail/1116587591                                                                                              |1              |
    //      |2015-07-22T10:47:38.076241Z|1.186.101.79 |2015-07-22T10:47:28.084661Z|1437562058|1437562048   |10        |0      |https://paytm.com:443/shop/orderdetail/1116587591                                                                                              |1              |
    //      |2015-07-22T10:47:38.352991Z|1.186.101.79 |2015-07-22T10:47:38.076241Z|1437562058|1437562058   |0         |0      |https://paytm.com:443/shop/action                                                                                                              |1              |
    //      |2015-07-22T10:47:51.042093Z|1.186.101.79 |2015-07-22T10:47:38.352991Z|1437562071|1437562058   |13        |0      |https://paytm.com:443/shop/summary/1116587591

    //Find unique URLs in a session
    //we can use count_distinct but this is faster
    sessionDf2.groupBy("client_ip","user_session_id").agg(approx_count_distinct("url").alias("distinct_urls")).orderBy(desc("distinct_urls"))show(10,false)
    //      +-------------+---------------+-------------+
    //      |client_ip    |user_session_id|distinct_urls|
    //      +-------------+---------------+-------------+
    //      |52.74.219.71 |5              |9526         |
    //      |119.81.61.166|6              |8756         |
    //      |52.74.219.71 |6              |5414         |
    //      |106.186.23.95|10             |4621         |
    //      |119.81.61.166|8              |4276         |
    //      |119.81.61.166|9              |3959         |
    //      |119.81.61.166|1              |3483         |
    //      |119.81.61.166|7              |2969         |
    //      |52.74.219.71 |9              |2928         |
    //      |119.81.61.166|10             |2804         |


    //Just to see some users with 2 sessions or more
    //sessionDf2.filter("user_session_id>='2'").show(20)

    //To find session length we need to find begining and end times of a session window. Ie client_ip & session_id grp by the start=min(ts) and end=max(ts)
    //val sessionWindow = Window.partitionBy("client","user_session_id")
    // val endTs = max("time").over(sessionWindow)
    //val startTs = min("time").over(sessionWindow)
    //sessionDf2.withColumn("beg_session",startTs).withColumn("end_session",endTs).show(100,false)
    import org.apache.spark.sql.functions._
    val sdf3 = sessionDf2.groupBy("client_ip","user_session_id")
                         .agg(org.apache.spark.sql.functions.max("epoch_ts").alias("end_time"),
                              org.apache.spark.sql.functions.min("epoch_ts").alias("beg_time"))
    //creating df with session len
    val sLenDf = sdf3.withColumn("ses_len",col("end_time")-col("beg_time"))
    sLenDf.show(100,false)
    //      +---------------+---------------+----------+----------+-------+
    //      |client_ip      |user_session_id|end_time  |beg_time  |ses_len|
    //      +---------------+---------------+----------+----------+-------+
    //      |1.186.143.37   |1              |1437581684|1437581676|8      |
    //      |1.187.164.29   |1              |1437533080|1437533011|69     |
    //      |1.22.41.76     |1              |1437583441|1437583379|62     |
    //      |1.23.208.26    |1              |1437586914|1437586901|13     |
    //      |1.23.208.26    |2              |1437588047|1437588047|0      |
    //      |1.23.36.184    |1              |1437542079|1437542064|15     |
    //      |1.38.19.8      |1              |1437581558|1437581498|60     |
    //      |1.38.20.34     |1              |1437555830|1437555638|192    |
    //      |1.39.13.13     |1              |1437588326|1437588321|5      |
    //      |1.39.32.249    |1              |1437562885|1437562828|57     |
    //      |1.39.32.249    |2              |1437583392|1437583300|92     |
    //      +---------------+---------------+----------+----------+-------+


    // Average session time across entire DF
    val avgSessionTotal = sLenDf.select(avg("ses_len"))
    avgSessionTotal.show(2,false)
    // The value is
    // Avg value is 101 seconds overall.
    //+------------------+
    //|avg(ses_len)      |
    //+------------------+
    //|100.64077415862131|
    //+------------------+

    //Average session time per client ip
    val avgSessionPerClient = sLenDf.groupBy("client_ip").avg("ses_len")
    avgSessionPerClient.show(20,false)
    //      +------------+------------+
    //      |client_ip   |avg(ses_len)|
    //      +------------+------------+
    //      |1.186.143.37|8.0         |
    //      |1.187.164.29|69.0        |
    //      |1.22.41.76  |62.0        |
    //      |1.23.208.26 |6.5         |
    //      |1.23.36.184 |15.0        |
    //      |1.38.19.8   |60.0        |
    //      |1.38.20.34  |192.0       |
    //      |1.39.13.13  |5.0         |
    //      |1.39.32.249 |74.5        |
    //      |1.39.32.59  |0.0         |
    //      |1.39.33.153 |41.0        |
    //      |1.39.33.33  |7.0         |
    //      |1.39.33.77  |9.0         |
    //      |1.39.34.4   |0.0         |
    //      |1.39.40.43  |170.0       |
    //      |1.39.60.37  |1103.0      |
    //      |1.39.61.53  |16.0        |
    //      |1.39.62.227 |17.0        |
    //      |1.39.63.197 |85.0        |
    //      |1.39.63.5   |89.0        |
    //      +------------+------------+


    //Most Engaged User by session
    //Here we consider most engaged user to be that ip which had the longest session not sum of sessions across an ip.
    sLenDf.orderBy(desc("ses_len")).show(10,false)
    //      +--------------+---------------+----------+----------+-------+
    //      |client_ip     |user_session_id|end_time  |beg_time  |ses_len|
    //      +--------------+---------------+----------+----------+-------+
    //      |119.81.61.166 |5              |1437563097|1437561028|2069   |
    //      |52.74.219.71  |5              |1437563097|1437561028|2069   |
    //      |106.186.23.95 |5              |1437563097|1437561028|2069   |
    //      |125.19.44.66  |5              |1437563096|1437561028|2068   |
    //      |125.20.39.66  |4              |1437563096|1437561028|2068   |
    //      |180.211.69.209|4              |1437563095|1437561028|2067   |
    //      |54.251.151.39 |5              |1437563097|1437561030|2067   |
    //      |192.8.190.10  |3              |1437563096|1437561029|2067   |
    //      |180.179.213.70|5              |1437563094|1437561028|2066   |
    //      |203.191.34.178|2              |1437563096|1437561030|2066   |
    //      +--------------+---------------+----------+----------+-------+

    //Most Engaged User by ip
    //Here we consider most engaged user to be that ip which had the longest sum(sessions) across all session it made since its a days data
    sLenDf.groupBy("client_ip").agg(sum("ses_len").alias("total_ses_len")).orderBy(desc("total_ses_len")).show(10,false)
    //      +--------------+-------------+
    //      |client_ip     |total_ses_len|
    //      +--------------+-------------+
    //      |220.226.206.7 |6795         |
    //      |52.74.219.71  |5258         |
    //      |119.81.61.166 |5253         |
    //      |54.251.151.39 |5236         |
    //      |121.58.175.128|4988         |
    //      |106.186.23.95 |4931         |
    //      |125.19.44.66  |4653         |
    //      |54.169.191.85 |4621         |
    //      |207.46.13.22  |4535         |
    //      |180.179.213.94|4512         |
    //      +--------------+-------------+
  }


  //convert string ts to epoch seconds
  val getEpoch:(String=>Long) = (arg:String) => {
    if(arg!=null)
      Instant.parse(arg).getEpochSecond()
    else
      -1
  }

  //extract URL without query string parts
  val parseUrlFromRequest:(String=>String) = (arg:String) =>  {
    val t = arg.split(" ")
    t(1)
    val k = t(1).split("\\?")
    k(0)
  }

  //run wrapper
  def run()={
    analyzeSession()
  }

}