﻿package com.turing.java.flink20.pipeline.demo13;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;

public class TumblingWindowDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //设置WebUI绑定的本地端口
        conf.setString(RestOptions.BIND_PORT,"8081");
        //使用配置
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);

        // 设置时间语义为 Event Time
        env.setStreamTimeCharacteristic(org.apache.flink.streaming.api.TimeCharacteristic.EventTime);

        // 使用 DataGeneratorSource 生成数据
        DataStream<String> text = env.addSource(new RichParallelSourceFunction<String>() {
            private boolean running = true;

            private Random random = new Random();

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (running) {
                    int randomNum = random.nextInt(5) + 1; // 生成1到5之间的随机数
                    long timestamp = System.currentTimeMillis(); // 获取当前时间作为时间戳
                    ctx.collect("key" + randomNum + "," + 1 + "," + timestamp);
                    ZonedDateTime generateDataDateTime = Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault());
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
                    String formattedGenerateDataDateTime = generateDataDateTime.format(formatter);
                    System.out.println("Generated data: " + "key" + randomNum + "," + 1 + "," + timestamp + " at " + formattedGenerateDataDateTime);
                    Thread.sleep(1000); // 每秒生成一条数据
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        });

        // 解析数据并提取时间戳
        DataStream<Tuple3<String, Integer, Long>> tuplesWithTimestamp = text.map(new MapFunction<String, Tuple3<String, Integer, Long>>() {
            @Override
            public Tuple3<String, Integer, Long> map(String value) {
                String[] words = value.split(",");
                return new Tuple3<>(words[0], Integer.parseInt(words[1]), Long.parseLong(words[2]));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT, Types.LONG));

        // 设置 Watermark 策略
        DataStream<Tuple3<String, Integer, Long>> withWatermarks = tuplesWithTimestamp.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Integer, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner((element, recordTimestamp) -> element.f2));

        // 窗口逻辑
        DataStream<Tuple2<String, Integer>> keyedStream = withWatermarks
                .keyBy(value -> value.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<Tuple3<String, Integer, Long>, Tuple2<String, Integer>, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<Tuple3<String, Integer, Long>, Tuple2<String, Integer>, String, TimeWindow>.Context context, Iterable<Tuple3<String, Integer, Long>> iterable, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        int count = 0;

                        // 遍历窗口内的所有元素并计数
                        for (Tuple3<String, Integer, Long> element : iterable) {
                            count++;
                        }

                        // 获取窗口的开始和结束时间
                        long start = context.window().getStart();
                        long end = context.window().getEnd();

                        // 将时间戳转换为 ZonedDateTime
                        ZonedDateTime startDateTime = Instant.ofEpochMilli(start).atZone(ZoneId.systemDefault());
                        ZonedDateTime endDateTime = Instant.ofEpochMilli(end).atZone(ZoneId.systemDefault());

                        // 格式化日期时间字符串
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
                        String formattedStart = startDateTime.format(formatter);
                        String formattedEnd = endDateTime.format(formatter);

                        // 打印窗口信息
                        System.out.println("Tumbling Window [ start " + formattedStart + ", end " + formattedEnd + ") for key " + s);

                        // 收集输出结果
                        collector.collect(new Tuple2<>(s, count));
                    }
                });

        // 输出结果
        keyedStream.print();

        // 执行任务
        env.execute("Tumbling Window Demo");
    }
}