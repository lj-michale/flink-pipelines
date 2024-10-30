package com.turing.java.flink19.pipeline.demo9

import java.sql.Connection
import com.clickhouse.jdbc.ClickHouseDataSource
import org.apache.flink.api.common.functions.OpenContext
import org.apache.flink.configuration.{CheckpointingOptions, Configuration}
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/**
  * @DESC: 读取 Kafka 2个不同的 topic 进行双流 join，把 join 结果写到 CK
  * @Auther:
  * @Date: 2024/10/28 21:19
  */
object Flink2StreamJoin2CK {

      def main(args: Array[String]): Unit = {

          /**1.19之后，新的设置CK方式*/
          val config = new Configuration()
          config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem")
          config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "hdfs:${checkpoint_dir}")

          val env = StreamExecutionEnvironment.getExecutionEnvironment
          env.enableCheckpointing(60000, CheckpointingMode.AT_LEAST_ONCE)
          env.configure(config)
          env.getCheckpointConfig.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION) //设置checkpoint记录的保留策略

          val tableEnv = StreamTableEnvironment.create(env)

          /**读取kafka数据源为t1*/
          tableEnv.executeSql(
              """
             Create table t1(
             |client_ip STRING,
             |domain STRING,
             |target_ip STRING
             |)
             |with(
             |'connector' = 'kafka',
             |'topic' = 'topic01',
              ...
             |)
            """.stripMargin)

          /**读取kafka数据源为t2*/
          tableEnv.executeSql(
              """
             Create table t2(
             |client_ip STRING,
             |nation STRING,
             |province STRING,
             |city STRING,
             |isp STRING
             |)
             |with(
             |'connector' = 'kafka',
             |'topic' = 'topic02',
              ...
             |)
            """.stripMargin)

          /**执行join操作,生成Table对象*/
          val tableResult = tableEnv.sqlQuery(
                                  """
                                  |select
                                  |t1.client_ip,
                                  |t2.nation,
                                  |t2.province,
                                  |t2.isp
                                  |from
                                  |t1
                                  |inner join t2
                                  |on
                                  |t1.client_ip=t2.client_ip
                                """.stripMargin)

          /**
           * 将Table转成DS
           * */
          tableEnv.toDataStream(tableResult)
            .addSink(new RichSinkFunction[Row] {
              override def open(openContext: OpenContext): Unit = {

              }

              override def invoke(value: Row,
                                  context: SinkFunction.Context): Unit = {

              }

              override def close(): Unit = {
                super.close()
              }
            })
//              .addSink(new RichSinkFunction[Row] {
//                  ...
//                  override def open(parameters: Configuration): Unit = {
//                      ...
//                  }
//
//                  override def invoke(value: Row, context: SinkFunction.Context): Unit = {
//                      ...
//                  }
//
//                  override def close(): Unit = super.close()
//              })

         env.execute()
      }
}