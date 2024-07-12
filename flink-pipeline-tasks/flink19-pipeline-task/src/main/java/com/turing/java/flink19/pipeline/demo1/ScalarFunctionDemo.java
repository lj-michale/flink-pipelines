package com.turing.java.flink19.pipeline.demo1;

import com.turing.java.flink19.bean.WaterSensor;
import com.turing.java.flink19.pipeline.demo1.function.MyHashFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class ScalarFunctionDemo {
    public static void main(String[] args) {
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
        // TODO 流 -> 表
        // 属性名 就是表的 字段名
        Table sensorTable = table_env.fromDataStream(sensorDS);
        table_env.createTemporaryView("sensor",sensorTable);

        //TODO 2. 注册函数
        table_env.createTemporaryFunction("hashFunction", MyHashFunction.class);

        // TODO 3. 调用自定义函数
        // 3.1 sql 用法
        table_env.sqlQuery("select hashFunction(id) from sensor")
                // 调用了 sql 的 execute 就不需要 env.execute()
                .execute()
                .print();
        // 3.2 table api 用法
        sensorTable
                // hashFunction的eval方法有注解的原因就是因为要告诉这里的参数类型
                .select(call("hashFunction",$("id")))
                .execute()
                .print();

    }
}