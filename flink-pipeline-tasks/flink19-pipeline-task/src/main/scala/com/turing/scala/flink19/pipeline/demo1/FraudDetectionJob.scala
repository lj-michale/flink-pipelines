package com.turing.scala.flink19.pipeline.demo1

import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.walkthrough.common.entity
import org.apache.flink.walkthrough.common.entity.Alert
import org.apache.flink.walkthrough.common.source.TransactionSource

//程序的数据处理流
object FraudDetectionJob {

  def main(args: Array[String]): Unit = {

    val env=StreamExecutionEnvironment.createLocalEnvironment()

    val transactions:DataStream[entity.Transaction]=env
      .addSource(new TransactionSource)
      .name("transactions")

    transactions.print()
    // transactions.print()
//    val alerts: DataStream[Alert] = transactions
//      .keyBy(transaction => transaction.getAccountId)
//      .process(new FraudDetector)
//      .name("fraud-detector")

//    alerts.print()
    //      .addSink(new AlertSink)
    //      .name("send-alerts")

    env.execute("Fraud Detection")
  }

}
