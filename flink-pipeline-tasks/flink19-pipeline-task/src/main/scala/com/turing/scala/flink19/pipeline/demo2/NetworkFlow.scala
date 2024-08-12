package com.turing.scala.flink19.pipeline.demo2

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp
import java.text.SimpleDateFormat
import scala.collection.mutable.ListBuffer

case class ApacheLogEvent(ip:String,userId:String,eventTime:Long,method:String,url:String)
case class UrlViewCount(url:String,windowEnd:Long,count:Long)

/**
 * @descr
 *    实时流量统计
 *    每隔 5 秒，输出最近 10 分钟内访问量最多的前 N 个 URL。
 *    （这个类似热门商品的统计）
 * */
object NetworkFlow {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val data = env.readTextFile("D:\\project\\flink-pipelines\\datset\\apache.log")
    val dataStream = data.map(line => {
      val arr = line.split(" ")
      val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val timestamp = simpleDateFormat.parse(arr(3)).getTime
      ApacheLogEvent(arr(0), arr(2), timestamp, arr(5), arr(6))
    })

    dataStream.print()

    env.execute()

  }


}
