package com.turing.java.flink19.pipeline.demo7;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @descr: ScalarFunction，标量函数，也就是一进一出的函数。
 * @desc: 需求：用自定义函数实现两数之和的操作，函数名叫：mySum
 */
public class Demo01_ScalarFunction {
    public static void main(String[] args) throws Exception {
        //1.构建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().set("parallelism.default","1");
        //重启策略的配置
        //Checkpoint的配置
        //状态后端的配置都需要在实际开发中写上，这里我们就省略。

        //2.数据源(source表）
        /*
            源表的Schema应该如下：
            |       num1      |       num2      |
            |        2        |         3       |
            |        3        |         4       |
            |        4        |         5       |
         */
        tEnv.executeSql("create table source_table (" +
                "num1 int," +
                "num2 int" +
                ") with (" +
                "'connector' = 'socket'" +
                ",'hostname' = 'node1'" +
                ",'port' = '9999'," +
                "'format' = 'csv'" +
                ")");

        //3.数据输出（sink表）
        /*
            |       num       |
            |        5        |
            |        7        |
            |        9        |
         */
        tEnv.executeSql("create table sink_table (" +
                "num int" +
                ") with (" +
                "'connector' = 'print'" +
                ")");

        //4.数据处理
        //4.1使用createTemporaryFunction方法来创建自定义函数，函数名为mySum
        tEnv.createTemporaryFunction("mySum",CustomScalarFunction.class);//这个就是把mySum函数注册到Flink集群中
        tEnv.executeSql("insert into sink_table select mySum(num1,num2) from source_table").await();

        //5.启动流式任务
        env.execute();
    }

    /**
     * 自定义的标量函数的子类，必须继承ScalarFunction，然后实现eval方法。
     * 这个eval方法可以有多个，看需求参数而定。
     * 这个类必须是public，让所有人都能访问
     */
    public static class CustomScalarFunction extends ScalarFunction {
        //必须实现eval方法
        public Integer eval(Integer a, Integer b) {
            return a + b;
        }
    }
}
