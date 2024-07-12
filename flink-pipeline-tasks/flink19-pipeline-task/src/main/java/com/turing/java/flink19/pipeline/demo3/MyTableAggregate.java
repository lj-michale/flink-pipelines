package com.turing.java.flink19.pipeline.demo3;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

// T: 返回类型（数值，排名） ACC：累加器类型(第一大的数，第二大的数)
public class MyTableAggregate extends TableAggregateFunction<Tuple2<Integer,Integer>,Tuple2<Integer,Integer>> {

    @Override
    public Tuple2<Integer, Integer> createAccumulator() {
        return Tuple2.of(0,0);
    }

    /**
     *
     * 每来一个数据 调用一次 判断比较大小 更新top2到acc中
     * @param acc 累加器类型
     * @param num 过来的数据
     */
    public void accumulate(Tuple2<Integer,Integer> acc,Integer num){
        if (num > acc.f0){
            // 新来的变第一，旧的第一变第二
            acc.f1 = acc.f0;
            acc.f0 = num;
        }else if (num > acc.f1){
            // 新来的变第二，旧的第二删除
            acc.f1 = num;
        }
    }

    /**
     * 输出结果 (数值，排名)
     * @param acc 累加器类型
     * @param out 采集器类型，和输出结果类型一样
     */
    public void emitValue(Tuple2<Integer,Integer> acc, Collector<Tuple2<Integer,Integer>> out){
        if (acc.f0 != 0){
            out.collect(Tuple2.of(acc.f0,1));
        }
        if (acc.f1 != 0){
            out.collect(Tuple2.of(acc.f1,2));
        }
    }
}