package com.duanhao.learn_wartermark

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironmentFactory
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

object WordCount {

  val lateData: OutputTag[Array[String]] = new OutputTag[Array[String]]("late")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //设置数据源
    val dataStream = env.socketTextStream("localhost", 10001)

    val waterMarkData = dataStream.map(_.split(","))
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Array[String]](Time.seconds(3)) {
        override def extractTimestamp(t: Array[String]): Long = {
          Integer.valueOf(t(0)) * 1000L
        }
      })

    val result = waterMarkData.keyBy(_ (1))
      .timeWindow(Time.seconds(15))
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(lateData)
      .reduce((d1,d2) => (
        if (d1(2).compareTo("1")==1) {d1} else d2
        ))

    result.getSideOutput(lateData).print("late data")
    result.print("正常数据")
  }
}
