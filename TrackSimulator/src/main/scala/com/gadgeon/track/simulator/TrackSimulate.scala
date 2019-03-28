package com.gadgeon.track.simulator

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.codehaus.jettison.json.JSONObject
import java.util.Date
import java.text.SimpleDateFormat

object TrackSimulate {

  def setupLogging() = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("com").setLevel(Level.ERROR)
  }

  def parseLine(line: String) = {
    val rand = new scala.util.Random
    val fields = line.split(",")
    val order = fields(1).toInt
    val imei = fields(0)
    val actual_date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
    val direction = 0.0f
    val lat = fields(2).toFloat
    val lon = fields(3).toFloat
    val odometer = 10.0f
    val speed = (10 + rand.nextInt(150)).toFloat //180.0f
    val analog = 0
    val temp = (25 + rand.nextInt(100)).toFloat //10.0f
    val eventCode = 0
    val textM = 0
    val fuel = 22.0f
    val temp2 = 0
    val voltage = 55.0f

    (order, imei, actual_date, lat, lon, direction, odometer, speed, analog, temp, eventCode, textM, fuel, temp2, voltage)
  }

  def main(args: Array[String]): Unit = {

    setupLogging()

    val KAFKA_SERVERS = "localhost:9092" //"JESMIPK-615:9092" // "localhost:9092" //"172.30.100.208:9092"//
    val RAW_TOPIC = "aaa-devicemessages"
    val TRACK_TOPIC = "aaa-mapdata"
    val SIMULATE_INTERVAL = 1000

    val props = new Properties()
    props.put("bootstrap.servers", KAFKA_SERVERS)
    props.put("client.id", "Producer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    //val conf = new SparkConf()
    //  .setAppName("TrackSimulator")
    //val sc = new SparkContext(conf)

    val sc = new SparkContext("local[2]", "TrackSimulator")

    val lines = sc.textFile("./data/tracking_data_all_ordred-imei.csv")
    //val lines = sc.textFile("./tracking_data_all_ordred-imei.csv")
    val header = lines.first()
    val data = lines.filter(row => row != header)
    val parsedLines = data.map(parseLine)
    var order = 1
    var path = "forward"
    while(1 == 1) {
      val result = parsedLines.filter(x => x._1 == order).collect
      result.foreach(x => {
        val json: JSONObject = new JSONObject();

        json.accumulate("IMEI", x._2)
        json.accumulate("ActualDate", x._3)
        json.accumulate("Lat", x._4)
        json.accumulate("Lon", x._5)
        json.accumulate("Direction", x._6)
        json.accumulate("Odotemer", x._7)
        json.accumulate("Speed", x._8)
        json.accumulate("Temp", x._10)
        json.accumulate("Fuel", x._13)
        json.accumulate("Voltage", x._15)

        val message = x._2.concat(",")
          .concat(x._3.toString).concat(",")
          .concat(x._4.toString).concat(",")
          .concat(x._5.toString).concat(",")
          .concat(x._6.toString).concat(",")
          .concat(x._7.toString).concat(",")
          .concat(x._8.toString).concat(",")
          .concat(x._9.toString).concat(",")
          .concat(x._10.toString).concat(",")
          .concat(x._11.toString).concat(",")
          .concat(x._12.toString).concat(",")
          .concat(x._13.toString).concat(",")
          .concat(x._14.toString).concat(",")
          .concat(x._15.toString)

        println(message)
        println(json)
        val producer = new KafkaProducer[String, String](props)
        val trackRecord: ProducerRecord[String, String] = new ProducerRecord(TRACK_TOPIC, json.toString)
        producer.send(trackRecord)

        val rawRecord: ProducerRecord[String, String] = new ProducerRecord(RAW_TOPIC, message)
        producer.send(rawRecord)
      })
      Thread.sleep(SIMULATE_INTERVAL)
      println(order)

      if(path == "forward") {
        order = order + 1
        if(order == 400) {
          path = "backward"
        }
      } else if(path == "backward") {
        order = order - 1
        if(order == 1) {
          path = "forward"
        }
      }
    }
  }
}
