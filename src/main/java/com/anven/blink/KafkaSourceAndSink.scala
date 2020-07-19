package com.anven.blink

import java.util.{Date, Properties}

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.table.api.{EnvironmentSettings, Table}

import scala.util.Random
import scala.util.parsing.json.JSONObject

//implicit conversion
import org.apache.flink.api.scala._
import org.apache.flink.table.api._

object KafkaSourceAndSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //kafka
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "node01:9092,node02:9092,node03:9092")
    properties.setProperty("zookeeper.connect", "node01:2181,node02:2181,node03:2181")
    properties.setProperty("group.id", "test")
    val stream = env.addSource(
      new FlinkKafkaConsumer[String]("test", new SimpleStringSchema(), properties));
    val words = stream.flatMap { _.split(" ") filter { !_.equals("fuck") } }
    words.print()

    //json
    val jsonStream = words.map { word => {
      JSONObject(Map("died" -> Random.nextInt(10),
      "positive" -> Random.nextInt(10),
      "ts" -> FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(new Date)
      )).toString()
    } }



    jsonStream.addSink(new FlinkKafkaProducer[String](
      "node01:9092,node02:9092,node03:9092",
      "pandemic",
      new SimpleStringSchema
    ))

    val counts = words.map { (_, 1) }
        .keyBy(0)
        .timeWindow(Time.seconds(10))
        .sum(1)
    counts.print()


    env.execute("kafka")
  }
}
