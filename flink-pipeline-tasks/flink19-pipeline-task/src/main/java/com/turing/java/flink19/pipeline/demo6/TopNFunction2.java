package com.turing.java.flink19.pipeline.demo6;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * 通过mapState实现topN方法
 * */
public class TopNFunction2 extends KeyedProcessFunction<String, Tuple2<String, Integer>, String> {

    private final int topSize;
    private MapState<Integer, String> itemState;

    public TopNFunction2(int topSize) {
        this.topSize = topSize;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MapStateDescriptor<Integer, String> itemStateDesc = new MapStateDescriptor<>(
                "itemState-state",
                TypeInformation.of(Integer.class),
                TypeInformation.of(String.class));
        itemState = getRuntimeContext().getMapState(itemStateDesc);
    }

    @Override
    public void processElement(
            Tuple2<String, Integer> value,
            Context ctx,
            Collector<String> out) throws Exception {

        // 更新状态
        itemState.put(value.f1, value.f0);

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
        for (Map.Entry<Integer, String> entry : itemState.entries()) {
            allItems.add(Tuple2.of(entry.getValue(), entry.getKey()));
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