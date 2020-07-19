package com.anven.sql

import java.nio.file.FileSystem
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.Schema
import org.apache.flink.table.sinks.{CsvTableSink, TableSink}


//implicit conversion
import org.apache.flink.api.scala._

object SqlDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val bsSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val bsTableEnv = StreamTableEnvironment.create(env, bsSettings)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "node01:9092,node02:9092,node03:9092")
    properties.setProperty("zookeeper.connect", "node01:2181,node02:2181,node03:2181")
    properties.setProperty("group.id", "test")

    val ds: DataStream[(String, String)] = env.addSource(
      new FlinkKafkaConsumer[String]("test",
        new SimpleStringSchema(),
        properties)
    ).map(line => {
      val strs = line.split(",")
      (strs(0), strs(1))
    })
    ds.print()
    bsTableEnv.createTemporaryView("pp", ds)
//    val table: Table = bsTableEnv.from("pp")
    val table = bsTableEnv.sqlQuery("select name, age from pp")
    bsTableEnv.registerTable("pp", table)

    val explanation: String = bsTableEnv.explain(table)
    println(s"fcuk you, $explanation")

    val sinkDDL =
      """
        CREATE TABLE people (
        name VARCHAR,
        age VARCHAR
        ) WITH (
                 'connector.type' = 'jdbc',
                 'connector.url' = 'jdbc:mysql://localhost:3306/big',
                 'connector.table' = 'people',
                 'connector.username' = 'root',
                 'connector.password' = 'qwertyuiop1234567890',
                 'connector.write.flush.max-rows' = '1'
        )
      """
    bsTableEnv.sqlUpdate(sinkDDL);

    bsTableEnv.sqlUpdate("insert into people " +
      "select name,age from pp");

    env.execute("SqlDemo")
  }
}
