package com.turing.java.flink19.pipeline.demo3;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class ScalarFunctionDemo3 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple3<String,Integer,Integer>> scoreDS = env.fromElements(
                Tuple3.of("燕双鹰",80,3),
                Tuple3.of("李大喜",90,4),
                Tuple3.of("李大喜",88,4),
                Tuple3.of("狄仁杰",95,4),
                Tuple3.of("狄仁杰",86,4)
        );

        // TODO 1.创建表环境
        StreamTableEnvironment table_env = StreamTableEnvironment.create(env);
        // TODO 流 -> 表
        // 属性名 就是表的 字段名
        Table scoreTable = table_env.fromDataStream(scoreDS,$("f0").as("name"),$("f1").as("score"),$("f2").as("weight"));// 字段名
        table_env.createTemporaryView("scores",scoreTable);

        //TODO 2. 注册函数
        table_env.createTemporaryFunction("weightAvg",MyWeightAvg.class);

        // TODO 3. 调用自定义函数
        table_env
                .sqlQuery("select name,weightAvg(score,weight) from scores group by name")
                .execute()
                .print();
    }
}