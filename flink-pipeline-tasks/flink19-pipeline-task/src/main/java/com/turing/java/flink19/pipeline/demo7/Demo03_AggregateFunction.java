package com.turing.java.flink19.pipeline.demo7;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * @author: Aggregate Function，聚合函数，类似于Hive中的聚合函数一样。也是多进一出的函数。
 * @desc: 需求：使用自定义函数实现单词统计的案例，函数名为myCount。
 */
public class Demo03_AggregateFunction {
    public static void main(String[] args) throws Exception {
        //1.构建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().set("parallelism.default","1");

        //2.source表构建
        /*
            source表的schema如下：
            |       word       |
            |       hello      |
            |       hive       |
            |       spark      |
            |       flink      |
         */
        tEnv.executeSql("create table source_table (" +
                "word string" +
                ") with (" +
                "'connector' = 'socket'," +
                "'hostname' = 'node1'," +
                "'port' = '9999'," +
                "'format' = 'csv'" +
                ")");


        //3.sink表构建
        /*
            sink表的schema如下：
            |       word       |        counts       |
            |       hello      |          1          |
            |       hive       |          2          |
            |       spark      |          3          |
            |       flink      |          4          |
         */
        tEnv.executeSql("create table sink_table (" +
                "word string," +
                "counts int" +
                ") with (" +
                "'connector' = 'print'" +
                ")");

        //4.数据处理
        //4.1 注册自定义函数到Flink集群中
        tEnv.createTemporaryFunction("myCount",CustomAggregateFunction.class);
        tEnv.executeSql("insert into sink_table select word,myCount(1) from source_table group by word").await();

        //5.启动流式任务
        env.execute();

    }

    /**
     * 自定义的聚合函数的类，用来实现聚合函数，这个类需要接收2个泛型类型：
     * 类型一：最终的结果类型，我们的需求是词频统计，因此最终的类型是int类型
     * 类型二：中间进行聚合结果类型，这里需要定义一个对象来进行接收
     */
    public static class CustomAggregateFunction extends AggregateFunction<Integer,CustomAccumlator> {

        /**
         * 创建累加器
         * @return 创建好的累加器
         */
        @Override
        public CustomAccumlator createAccumulator() {
            return new CustomAccumlator();
        }

        /**
         * 进行中间结果的逻辑计算
         * @param accumulator 累加器中已经保留的中间聚合结果
         * @param i 当前输入的数据的值
         */
        public void accumulate(CustomAccumlator accumulator, Integer i) {
            accumulator.count = accumulator.count + 1;//i是当前的单词出现的次数，因此在这里可以用+i或者+1都行。
        }

        /**
         * 返回最终的累加结果
         * @param accumulator 累加器
         * @return 返回的结果
         */
        @Override
        public Integer getValue(CustomAccumlator accumulator) {
            return accumulator.count;
        }
    }

    /**
     * 自定义的中间聚合结果的类,用来保存中间的聚合结果,它的名字叫做累加器，可以做各种操作
     */
    public static class CustomAccumlator {
        //这个属性就是用来保存中间结果的，就是用来做累加计算的
        public Integer count = 0;
    }
}

