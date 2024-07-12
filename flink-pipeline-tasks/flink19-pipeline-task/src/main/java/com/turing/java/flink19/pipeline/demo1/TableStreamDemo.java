package com.turing.java.flink19.pipeline.demo1;

import com.turing.java.flink19.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class TableStreamDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 2L, 2),
                new WaterSensor("s2", 2L, 21),
                new WaterSensor("s3", 3L, 3),
                new WaterSensor("s3", 4L, 4)
        );


        // TODO 1.创建表环境
        StreamTableEnvironment table_env = StreamTableEnvironment.create(env);

        // TODO 1. 流 -> 表
        // 属性名 就是表的 字段名
        Table sensorTable = table_env.fromDataStream(sensorDS);
        // 或者指定保留哪些字段
//        table_env.fromDataStream(sensorDS,$("id"));
        // 注册
        table_env.createTemporaryView("sensor",sensorTable);

        Table result = table_env.sqlQuery("select id,sum(vc) from sensor group by id");
        Table filter = table_env.sqlQuery("select id,ts,vc from sensor where ts > 2");

        // TODO 2. 表 -> 流
        // 2.1 追加流
        table_env.toDataStream(filter, WaterSensor.class).print("filter");
        // 2.2 更新流(结果需要更新)
        table_env.toChangelogStream(result).print("sum");

        // 只要代码中调用了 DataStream 就需要使用 execute
        env.execute();
    }
}