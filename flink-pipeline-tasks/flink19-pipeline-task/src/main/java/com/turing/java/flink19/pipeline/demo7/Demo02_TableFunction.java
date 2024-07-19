package com.turing.java.flink19.pipeline.demo7;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;

/**
 * @author:  TableFunction，表值函数，也就是一进多出的函数。
 * @desc: 需求：实现一个类似于flatMap的函数，一个数字进去，多个数字出来。函数名为myFlatMap。
 */
public class Demo02_TableFunction {
    public static void main(String[] args) throws Exception {
        //1.构建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().set("parallelism.default","1");

        //2.构建source表
        /*
            source表的schema
            |       num      |
            |        3       | -> 0,1,2
            |        4       | -> 0,1,2,3
         */
        tEnv.executeSql("create table source_table (" +
                "num int" +
                ") with (" +
                "'connector' = 'socket'," +
                "'hostname' = 'node1'," +
                "'port' = '9999'," +
                "'format' = 'csv'" +
                ")");

        //3.构建sink表
        /*
            sink表的schema
            |      num      |
            |       0       |
            |       1       |
            |       2       |
            |       0       |
            |       1       |
            |       2       |
            |       3       |
         */
        tEnv.executeSql("create table sink_table (" +
                "num int" +
                ") with (" +
                "'connector' = 'print'" +
                ")");

        //4.数据处理
        //4.1 把自定义的函数注册到Flink集群中，以供后面使用
        tEnv.createTemporaryFunction("myFlatMap",CustomTableFunction.class);
        /**
         * left join lateral table(myFlatMap(num)) as tmp(num1) on true
         * left join lateral table：固定写法
         * myFlatMap：自定义的函数
         * num：自定义的函数作用在哪个列上，这里指定为作用在source_table的num列上
         * tmp：临时表，只有一个num1列
         * on true：必填项
         */
        tEnv.executeSql("insert into sink_table select num1 from source_table left join lateral table(myFlatMap(num)) as tmp(num1) on true").await();

        //5.启动流式任务
        env.execute();

    }

    /**
     * 自定义类，用来实现table function
     * 泛型：The type of the output row，也就是输出的数据类型，这里由于输出的是int类型，因此给定Integer
     */
    public static class CustomTableFunction extends TableFunction<Integer> {
        /**
         * eval方法，用来实现自定义的逻辑的
         * @param num 输入的参数，也就是数字
         */
        public void eval(Integer num) {
            for (int i = 0; i < num; i++) {
                collect(i);
            }
        }
    }
}

