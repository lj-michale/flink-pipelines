﻿package com.turing.java.flink20.pipeline.demo13;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class ProcessTimeDemo {public static void main(String[] args) throws Exception {
    // 创建流处理环境
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    // 定义输入数据
    DataStream<Event> events = env.fromElements(
            new Event("user1", "/page1"),
            new Event("user2", "/page2"),
            new Event("user3", "/page3"),
            new Event("user3", "/page3")
    );

    // 定义窗口逻辑
    DataStream<Tuple3<String, Integer, Long>> result = events
            .keyBy(event -> event.user)
            .window(TumblingProcessingTimeWindows.of(Time.milliseconds(1)))
            .process(new ProcessWindowFunction<Event, Tuple3<String, Integer, Long>, String, TimeWindow>() {
                @Override
                public void process(String s, Context context, Iterable<Event> elements, Collector<Tuple3<String, Integer, Long>> out) throws Exception {
                    int count = 0;
                    for (Event event : elements) {
                        count++;
                    }
                    // 获取窗口的结束时间
                    long end = context.window().getEnd();
                    out.collect(new Tuple3<>(s, count, end));
                }
            });

    // 输出结果
    result.print();

    // 执行任务
    env.execute("Processing Time Demo");
}

    public static class Event {
        public String user;
        public String page;

        public Event() {}

        public Event(String user, String page) {
            this.user = user;
            this.page = page;
        }
    }
}