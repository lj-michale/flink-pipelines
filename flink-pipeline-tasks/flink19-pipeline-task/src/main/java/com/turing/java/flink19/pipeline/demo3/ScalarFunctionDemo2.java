package com.turing.java.flink19.pipeline.demo3;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class ScalarFunctionDemo2 {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> wordsDS = env.fromElements(
                "hello flink",
                "hello hadoop",
                "hello kafka",
                "hello spark",
                "hello hive and impala"
        );

        // TODO 1.创建表环境
        StreamTableEnvironment table_env = StreamTableEnvironment.create(env);
        // TODO 流 -> 表
        // 属性名 就是表的 字段名
        Table sensorTable = table_env.fromDataStream(wordsDS,$("name"));// 字段名
        table_env.createTemporaryView("words",sensorTable);

        //TODO 2. 注册函数
        table_env.createTemporaryFunction("splitFunction",MySplitFunction.class);

        // TODO 3. 调用自定义函数
        // 交叉用法
        table_env.sqlQuery("select word,length from words,lateral table (splitFunction(name))")
                // 调用了 sql 的 execute 就不需要 env.execute()
                .execute()
                .print();
        // 左联结
        table_env
                .sqlQuery("select name,word,length from words left join lateral table (splitFunction(name)) on true")
                .execute()
                .print();

    }
}