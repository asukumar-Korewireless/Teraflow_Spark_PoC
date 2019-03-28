package com.gadgeon.track.simulator

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.{Level, Logger}


object Simulator {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("com").setLevel(Level.ERROR)
    val imeis = List("AA001", "AA002", "AA003", "AA004")
    val rand = new scala.util.Random
    val KAFKA_SERVERS = "localhost:9092"
    val RAW_TOPIC = "test"
    val SIMULATE_INTERVAL = 10000

    val props = new Properties()
    props.put("bootstrap.servers", KAFKA_SERVERS)
    props.put("client.id", "Producer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    while (true) {
      var speed = (10 + rand.nextInt(150)).toFloat.toString
      val random_index = rand.nextInt(imeis.length)
      var imei = imeis(random_index)
      var unprocessedcreated = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new Date)
      var actualdate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new Date)
      var message = "{\"Sensors\":[{\"Name\":\"MessageType\",\"Value\":\"GTFRI\",\"Type\":\"Other\",\"SensorID\"" +
        ":315,\"UnitOfMeasurement\":28},{" +
        "\"Name\":\"Col1\",\"Value\":\"GTFRI\"," +
        "\"Type\":\"Other\",\"SensorID\":101,\"UnitOfMeasurement\":28},{" +
        "\"Name\":\"Event\",\"Value\":\"GTFRI\",\"Type\":\"Other\",\"SensorID\":2,\"UnitOfMeasurement\":28},{" +
        "\"Name\":\"DeviceType\",\"Value\":\"27\",\"Type\":\"Other\",\"SensorID\":551,\"UnitOfMeasurement\":28},{" +
        "\"Name\":\"ProtocolVersion\",\"Value\":\"0501\",\"Type\":\"Other\",\"SensorID\":558,\"UnitOfMeasurement\":28},{" +
        "\"Name\":\"GPSFix\",\"Value\":\"A\",\"Type\":\"Other\",\"SensorID\":3," +
        "\"UnitOfMeasurement\":28},{\"Name\":\"HDOP\",\"Value\":\"1\",\"Type\":\"Other\"," +
        "\"SensorID\":241,\"UnitOfMeasurement\":28},{\"Name\":\"Speed\",\"Value\":\"" + speed +"\",\"Type\":"+
        "\"Speed\",\"SensorID\":9,\"UnitOfMeasurement\":24}," +
        "{\"Name\":\"Azimuth\",\"Value\":\"9\",\"Type\":\"Direction\"," +
        "\"SensorID\":7,\"UnitOfMeasurement\":22}," +
        "{\"Name\":\"Altitude\",\"Value\":\"2320.8\"" +
        ",\"Type\":\"Length\",\"SensorID\":8,\"UnitOfMeasurement\":2}," +
        "{\"Name\":\"Longitude\",\"Value\":\"-99.215261\",\"Type\":\"Other\",\"SensorID\":6," +
        "\"UnitOfMeasurement\":21},{\"Name\":\"Latitude\",\"Value\":\"19.584815\",\"Type\":\"Other" +
        "\",\"SensorID\":5,\"UnitOfMeasurement\":21},{\"Name\":\"UTCTime\",\"Value\":\"01/11/2019 22:24:40\"," +
        "\"Type\":\"Other\",\"SensorID\":700,\"UnitOfMeasurement\":28},{\"Name\":\"CellInfo1\"," +
        "\"Value\":\"CellID:98502080|LAC:9529|MCC:820|MNC:3\",\"Type\":\"Other\"," +
        "\"SensorID\":511,\"UnitOfMeasurement\":28},{\"Name\":\"Odometer\",\"Value\":\"102755.9\"," +
        "\"Type\":\"Length\",\"SensorID\":201,\"UnitOfMeasurement\":0},{\"Name\":" +
        "\"BatteryLevel\",\"Value\":\"100\",\"Type\":\"Percentage\",\"SensorID\":10,\"UnitOfMeasurement\":17}," +
        "{\"Name\":\"MotionStatus\",\"Value\":\"17\",\"Type\":\"Other\",\"SensorID\":313,\"UnitOfMeasurement\":28}," +
        "{\"Name\":\"IgnitionStatus\",\"Value\":\"1\",\"Type\":\"Other\",\"SensorID\":133,\"UnitOfMeasurement\":28}," +
        "{\"Name\":\"DigitalInputStatus\",\"Value\":\"00000000\",\"Type\":\"Other\",\"SensorID\":12,\"UnitOfMeasurement\":28}," +
        "{\"Name\":\"DigitalOutputStatus\",\"Value\":\"00000000\",\"Type\":\"Other\",\"SensorID\":13,\"UnitOfMeasurement\":28}" +
        ",{\"Name\":\"SendTime\",\"Value\":\"01/11/2019 22:26:57\",\"Type\":\"Other\",\"SensorID\":701," +
        "\"UnitOfMeasurement\":28},{\"Name\":\"SequenceNumber\",\"Value\":\"2933\",\"Type\":\"Other\"," +
        "\"SensorID\":15,\"UnitOfMeasurement\":28},{\"Name\":\"IsGPSValid\",\"Value\":\"True\",\"Type\":\"Other\"," +
        "\"SensorID\":4,\"UnitOfMeasurement\":28}],\"Guid\":\"2977655f-b576-4666-8d20-c4afff094aa2\"," +
        "\"IMEI\":\"" + imei + "\",\"ActualDate\":\"" + actualdate + "\",\"HardwareName\":\"QUECLINKGV300\"," +
        "\"UnprocessedCreated\":\"" + unprocessedcreated + "\",\"PortNumber\":11003,\"ServerName\":\"DALVAREZ-691\"," +
        "\"TenantName\":\"itrac\",\"MessageType\":\"KOREPL\"}"
      val producer = new KafkaProducer[String, String](props)
      val rawRecord: ProducerRecord[String, String] = new ProducerRecord(RAW_TOPIC, message)
      producer.send(rawRecord)
      Thread.sleep(SIMULATE_INTERVAL);

    }
  }
}