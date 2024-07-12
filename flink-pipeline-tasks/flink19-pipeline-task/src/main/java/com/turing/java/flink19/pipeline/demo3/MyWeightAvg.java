package com.turing.java.flink19.pipeline.demo3;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.AggregateFunction;

// 泛型 T：返回类型 ACC: 累加器类型<加权总和,权重总和>
public class MyWeightAvg extends AggregateFunction<Double, Tuple2<Integer,Integer>> {

    /**
     * 计算累加和
     * @param acc 累加器的类型 固定写法
     * @param score 分神
     * @param weight 权重
     */
    public void accumulate(Tuple2<Integer, Integer> acc,Integer score,Integer weight){
        acc.f0 += score*weight;
        acc.f1 += weight;
    }

    @Override
    public Double getValue(Tuple2<Integer, Integer> integerIntegerTuple2) {
        return integerIntegerTuple2.f0*1d/integerIntegerTuple2.f1;
    }

    @Override
    public Tuple2<Integer, Integer> createAccumulator() {
        return Tuple2.of(0,0);
    }
}