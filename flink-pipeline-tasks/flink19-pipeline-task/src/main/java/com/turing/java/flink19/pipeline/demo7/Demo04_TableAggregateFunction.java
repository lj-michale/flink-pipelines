package com.turing.java.flink19.pipeline.demo7;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

/**
 * @author: Table Aggregate Function，表值聚合函数，它是多进多出的函数。
 * @desc: 需求：以单词结果为例，求top2，使用自定义函数实现。
 */
public class Demo04_TableAggregateFunction {
    public static void main(String[] args) throws Exception {
        //1.构建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().set("parallelism.default","1");

        //2.source表构建
        /*
            source表的schema如下：
            |       word        |       counts      |
            |       hello       |         3         | -> hello,3
            |       hello       |         2         | -> hello,3   hello,2
            |       hello       |         4         | -> hello,4   hello,3
         */
        tEnv.executeSql("create table source_table (" +
                "word string," +
                "counts int" +
                ") with (" +
                "'connector' = 'socket'," +
                "'hostname' = 'node1'," +
                "'port' = '9999'," +
                "'format' = 'csv'" +
                ")");

        //3.sink表构建
        /*
            sink表的schema如下：
            |       word        |       counts      |
            |       hello       |          3        |
            |       hello       |          3        |
            |       hello       |          2        |
            |       hello       |          4        |
            |       hello       |          3        |
         */
        tEnv.executeSql("create table sink_table (" +
                "word string," +
                "counts int" +
                ") with (" +
                "'connector' = 'print'" +
                ")");

        //4.数据处理
        //4.1 注册top2函数，给后面的代码使用
        tEnv.createTemporaryFunction("top2",CustomTableAggregateFunction.class);
        //这里没办法使用纯SQL来实现了，必须使用Table API来实现。
        tEnv.from("source_table")//读取源表
                .groupBy(Expressions.$("word"))//对表数据进行分组,这里采用word进行分组
                //flatAggregate：表值聚合必须采用flatAggregate方法来实现
                /**
                 * call就是调用自定义函数,里面接收2个参数：
                 * 参数一：函数名称，这里指定为top2函数
                 * 参数二：函数作用在哪个列上，这里作用在源表中的counts列
                 */
                .flatAggregate(Expressions.call("top2", Expressions.$("counts")).as("num"))//处理完后给个num别名
                .select(Expressions.$("word"),Expressions.$("num"))//num列来自于上个处理的num别名
                .executeInsert("sink_table")
                .await();

        //5.启动流式任务
        env.execute();

    }

    /**
     * 自定义的类，用来集成表值聚合函数，实现自定义的逻辑
     * 泛型一：最终结果类型，这里是int类型
     * 泛型二：是中间的聚合结果类型，必须定义一个类来实现，这个类就是累加器了
     */
    public static class CustomTableAggregateFunction extends TableAggregateFunction<Integer, CustomAccmulator> {
        /**
         * 创建累加器的方法
         * @return 创建好的累加器对象
         */
        @Override
        public CustomAccmulator createAccumulator() {
            return new CustomAccmulator();
        }

        /**
         * 对数据进行累计计算
         * @param accumulator 累加器对象，里面保存着top2的结果
         * @param num 数据，这个数据要和历史的top2结果进行计算
         */
        public void accumulate(CustomAccmulator accumulator,Integer num) {
            if (num > accumulator.first) {
                //如果数据大于累加器中的最大值，则这个数据应该保留下来，用first来进行接收
                //注意：如果要用first来接收num数据，则应该先把first赋值给second
                //(hadoop,3)(hadoop,2)
                accumulator.second = accumulator.first;//把first赋值给second
                accumulator.first = num;//把num赋值给first
            } else if (num > accumulator.second) {
                //如果num大于second，则应该把second替换为num
                //(hadoop,5),(hadoop,3)
                accumulator.second = num;
            }
        }

        /**
         * 返回最终结果
         * @param accumulator 累加器，最新结果保存在累加器中，要从累加器中去取
         * @param out 把最终结果返回
         */
        public void emitValue(CustomAccmulator accumulator, Collector<Integer> out) {
            //返回之前，应该先判断一下，看是否有数据
            if(accumulator.first != Integer.MIN_VALUE) {
                //能进来，表示first有数据，应该输出
                out.collect(accumulator.first);
            }
            if(accumulator.second != Integer.MIN_VALUE) {
                //如果能进来，则表示second也是有数据的，应该输出
                out.collect(accumulator.second);
            }
        }
    }

    public static class CustomAccmulator {
        public Integer first = Integer.MIN_VALUE;//保存第一个值
        public Integer second = Integer.MIN_VALUE;//保存第二个值
    }
}
