package com.turing.java.flink19.pipeline.demo3;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class ScalarFunctionDemo4 {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> valDS = env.fromElements(3,5,4,7,5,6,1,4,2);

        // TODO 1.创建表环境
        StreamTableEnvironment table_env = StreamTableEnvironment.create(env);
        // TODO 流 -> 表
        // 属性名 就是表的 字段名
        Table valueTable = table_env.fromDataStream(valDS,$("value"));// 字段名
        table_env.createTemporaryView("values",valueTable);

        //TODO 2. 注册函数
        table_env.createTemporaryFunction("top2",MyTableAggregate.class);

        // TODO 3. 调用自定义函数: 只支持 table api
        valueTable
                .flatAggregate(call("top2",$("value")).as("value","rank"))
                .select($("value"),$("rank"))
                .execute()
                .print();
    }
}