package com.gadgeon.koreone
import java.io.PrintStream
import java.net.{InetAddress, Socket}
import java.util
import java.util.{Properties, TimeZone}

import com.datastax.driver.core.utils.UUIDs
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.streaming._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.codehaus.jettison.json.JSONObject
import com.datastax.spark.connector._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.BufferedSource

object PortwiseProcess {

  var WindowList = new ListBuffer[Long]()
  val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")


  def setupLogging() = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("com").setLevel(Level.ERROR)
  }

  def parseLine(line: String) = {
    val json = new JSONObject(line)
    val portNumber = json.get("PortNumber")
    val imei = json.get("IMEI")
    val ActualDate = json.get("ActualDate").toString
    val UnprocessedCreated = json.get("UnprocessedCreated").toString
    val sensors = json.getJSONArray("Sensors")
    val speedSensors = (0 until sensors.length).map(sensors.getJSONObject)
                       .filter(x => x.get("Name")== "Speed")
    var speed = 0f;
      speedSensors.foreach(s => {
      speed = s.getDouble("Value").toFloat
    })
    println(imei +"," + speed +"," +ActualDate + "," + UnprocessedCreated)
    (imei,portNumber, speed, ActualDate, UnprocessedCreated)
  }

  def writeToTcp(message: String) = {
    val SOCKET_HOST="192.168.65.135"
    val fields = message.split(",")
    val port = fields(1).toInt
    val s = new Socket(InetAddress.getByName(SOCKET_HOST), port)
    lazy val in = new BufferedSource(s.getInputStream()).getLines()
    val out = new PrintStream(s.getOutputStream())
    println("Speed limit exceeded -> TCP")
    out.println(message)
    out.flush()
    in.next()
    s.close()
  }

  def main(args: Array[String]) = {

    val CASSANDRA_HOST = "localhost"
    val KAFKA_SERVERS =  "localhost:9092"
    val INPUT_TOPIC = "test"
    val OUTPUT_TOPIC = "aaa-speedanalysis"
    val CASSANDRA_KEYSPACE = "koreone"
    val CASSANDRA_TABLE1 = "speed"
    val CASSANDRA_TABLE2 = "sample"
    val CASSANDRA_COLUMNS1 = SomeColumns("id","imei","portno","speed", "actualdate","unprocessedcreateddate")
    val CASSANDRA_COLUMNS2= SomeColumns("id","imei","avgspeed","windowendtime")
    val CHECKPOINT_PATH = "c:\\tmp"
    val SPEED_LIMIT = 110
    val window_duration = 120;
    setupLogging()

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("KafkaStreamToCassandra")
      .set("spark.cassandra.connection.host", CASSANDRA_HOST)
    val ssc = new StreamingContext(conf, Seconds(1))
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> KAFKA_SERVERS,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val props = new Properties()
    props.put("bootstrap.servers", KAFKA_SERVERS)
    props.put("client.id", "Producer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val topics = Array(INPUT_TOPIC)
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    val imei_list =  List("AA001","AA002","AA003","AA004")
    val avgspeed = ssc.sparkContext.parallelize(imei_list).map(x=>(x,0f))
    val lines = stream.map(_.value)
    val parsedLine = lines.map(parseLine)
    val cachedStream = parsedLine.cache()
    var first :Long = 0 ;
    var last : Long = 0;
    var window_time = ((System.currentTimeMillis()/1000)).toString
    var WindowData = new ListBuffer[String]()
    val reprocessrdd =  parsedLine.filter(l=> {
      println(("reprocess_filter :"+dateFormat.parse(l._5).getTime/1000).toString + "," + (window_time.toLong).toString )
      dateFormat.parse(l._5).getTime/1000 < window_time.toLong
    })
    reprocessrdd.foreachRDD(z=>z.foreach(k=>println("REPROCESSED RDD :" + k)))
    val reprocessrdd2 = reprocessrdd.map(a=>(a._1, dateFormat.parse(a._5).getTime/1000 + window_duration ,a._3 ))
    reprocessrdd2.foreachRDD(rdd=>rdd.foreachPartition(a=>{
      a.foreach(b=>{
       println("REPROCESSED RDD 2 :" +b)
        last = b._2
        first = b._2 - window_duration
        WindowList.foreach(k=>println("WINDOW LIST :" + k))
        WindowList.filter(x=>(x> first && x < last)).foreach(z=>
        { println("OUR WINDOW :" + z)
          val producer1 = new KafkaProducer[String, String](props)
          var msg = b._1+","+z.toString+","+(z-window_duration).toString +"," +b._3.toString
          println(msg)
          val record1:ProducerRecord[String, String] = new ProducerRecord("test2", msg)
          producer1.send(record1)
      })
    })}))
    val avgFn = (x:(Float, Int), y: (Float, Int)) => (x._1 + y._1, x._2 + y._2)
    val speedData = cachedStream.filter(y=>({
      println(("avg_filter  :"+dateFormat.parse(y._5).getTime/1000).toString + "," + (window_time.toLong - window_duration).toString )
      dateFormat.parse(y._5).getTime/1000 > window_time.toLong - window_duration
    }))
      .map(x => (x._1, x._3)).mapValues(s => (s, 1))
    val averageSpeed = speedData.reduceByKeyAndWindow(avgFn,Seconds(window_duration),Seconds(window_duration))
      .mapValues((x) => x._1 / x._2)
      .map(x => (x._1, x._2))
   val inputData: mutable.Queue[RDD[(String,Float)]] = mutable.Queue()
    averageSpeed.foreachRDD((rdd,time)=> {
      window_time = (time.milliseconds/1000).toString
      WindowList += time.milliseconds/1000
      inputData += avgspeed
    }
    )

    val inputStream: InputDStream[(String,Float)] = ssc.queueStream[(String,Float)](inputData)
     val inputStream2 = inputStream.window(Seconds(window_duration),Seconds(window_duration)).cache()
    val avgspeed_dstream = averageSpeed.map(a=>(a._1.toString,a._2)).union(inputStream2)
    avgspeed_dstream.foreachRDD(a=>a.foreach(b=>println("avgdssd :"+b)))
    val avg_speed_reduced = avgspeed_dstream.reduceByKey((a,b)=>a+b)
    avg_speed_reduced.map(a=>(UUIDs.timeBased(),a._1,a._2,dateFormat.format(window_time.toLong*1000l))).saveToCassandra(CASSANDRA_KEYSPACE, CASSANDRA_TABLE2, CASSANDRA_COLUMNS2)
    parsedLine.map(x=>(UUIDs.timeBased(),x._1,x._2,x._3,x._4,x._5)).saveToCassandra(CASSANDRA_KEYSPACE, CASSANDRA_TABLE1, CASSANDRA_COLUMNS1)

    val overSpeedData = parsedLine.filter(s => s._3 >= SPEED_LIMIT)
    overSpeedData.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val producer = new KafkaProducer[String, String](props)
        if(partition.hasNext) {
          partition.foreach(overSpeedItem => {
            val message = overSpeedItem._1.toString().concat(",")
              .concat(overSpeedItem._2.toString).concat(",")
              .concat(overSpeedItem._3.toString)
            println(message)
            val record:ProducerRecord[String, String] = new ProducerRecord(OUTPUT_TOPIC, message)
            producer.send(record)
            // Write the overspeed data to TCP port
            writeToTcp(message)
          })
        }
      })
    })
    ssc.checkpoint(CHECKPOINT_PATH)
    ssc.start()
    ssc.awaitTermination()
  }
}

