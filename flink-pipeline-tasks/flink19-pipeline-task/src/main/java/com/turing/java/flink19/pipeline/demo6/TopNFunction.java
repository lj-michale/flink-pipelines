package com.turing.java.flink19.pipeline.demo6;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 通过listState实现topN方法
 * */
public class TopNFunction extends KeyedProcessFunction<String, Tuple2<String, Integer>, String> {

    private final int topSize;
    private ListState<Tuple2<String, Integer>> itemState;

    public TopNFunction(int topSize) {
        this.topSize = topSize;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ListStateDescriptor<Tuple2<String, Integer>> itemStateDesc = new ListStateDescriptor<>(
                "itemState-state",
                TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));
        itemState = getRuntimeContext().getListState(itemStateDesc);
    }

    @Override
    public void processElement(
            Tuple2<String, Integer> value,
            Context ctx,
            Collector<String> out) throws Exception {

        // 更新状态
        itemState.add(value);

        // 注册定时器
        ctx.timerService().registerEventTimeTimer(value.f1 + 1);
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<String> out) throws Exception {

        // 获取所有的键值对
        List<Tuple2<String, Integer>> allItems = new ArrayList<>();
        for (Tuple2<String, Integer> item : itemState.get()) {
            allItems.add(item);
        }

        // 清除状态
        itemState.clear();

        // 按照得分排序并取前N个
        allItems.sort(new Comparator<Tuple2<String, Integer>>() {
            @Override
            public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
                return o2.f1.compareTo(o1.f1);
            }
        });

        StringBuilder result = new StringBuilder();
        for (int i = 0; i < Math.min(topSize, allItems.size()); i++) {
            Tuple2<String, Integer> currentItem = allItems.get(i);
            result.append("No ").append(i).append(":")
                    .append(" Key ").append(currentItem.f0)
                    .append(" Value ").append(currentItem.f1)
                    .append("\n");
        }

        out.collect(result.toString());
    }
}