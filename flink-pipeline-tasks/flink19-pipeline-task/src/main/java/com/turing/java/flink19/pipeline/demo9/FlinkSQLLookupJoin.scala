
package com.turing.java.flink19.pipeline.demo9

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * @DESC: 利用Flink SQL 的 lookup join 实现对 Doris 的点查
 * @Date: 2024/7/31 20:29
 */
object FlinkSQLLookupJoin {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRestartStrategy(RestartStrategies.noRestart())

    /**因为数据有界，用batch模式*/
    env.setRuntimeMode(RuntimeExecutionMode.BATCH)
    val tableEnv = StreamTableEnvironment.create(env)

    import org.apache.flink.streaming.api.scala._

    /**
     * 模拟生成主数据，实际可以是任何数据源，比如Kafka等
     * */
    val mainDS = env.fromCollection(Seq(
      (1,"221.198.68.4"),
      (2, "192.168.100.1"),
      (3, "110.22.68.4"),
      (4, "172.64.33.195"),
      (5, "20.242.68.3"),
      (6, "1.2.3.99")
    ))

    /**
     * 将主数据映射成表t1
     * */
    val mainTable = tableEnv.fromDataStream(mainDS).as("id","ip")
    tableEnv.createTemporaryView("t1", mainTable)

    /**
     * 将Doris外部数据源映射成表t2
     * */
    tableEnv.executeSql(
      """
        |CREATE TABLE t2 (
        |`client_ip` STRING,
        |domain STRING,
        |`time` STRING,
        |target_ip STRING,
        |rcode INT,
        |query_type INT,
        |authority_record STRING,
        |add_msg STRING,
        |dns_ip STRING
        |)
        |    WITH (
        |      'connector' = 'doris',
        |      'fenodes' = '192.168.221.173:8030',
        |      'table.identifier' = 'example_db.dns_logs_from_kafka',
        |       'jdbc-url' = 'jdbc:mysql://192.168.221.173:9030',
        |      'username' = 'root',
        |      'password' = '****',
        |     'sink.label-prefix' = 'label_001'
        |)
            """.stripMargin)

    /**
     * 执行最后的查询统计操作，并打印结果
     * */
    tableEnv.executeSql(
      """
        |select
        |ip,
        |SUM(case when domain is null then 0 else 1 END) as cnt
        |from
        |(
        |select
        |a.ip,
        |b.domain
        |from
        |(
        |select
        |ip,
        |PROCTIME() AS proctime
        |from
        |t1
        |) AS a
        |left join
        |t2 FOR SYSTEM_TIME AS OF a.proctime  AS b
        |on a.ip = b.target_ip
        |)
        |group by
        |ip
        """.stripMargin).print()
  }

}