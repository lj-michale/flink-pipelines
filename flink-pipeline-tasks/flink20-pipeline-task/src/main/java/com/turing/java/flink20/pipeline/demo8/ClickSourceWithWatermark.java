package com.turing.java.flink20.pipeline.demo8;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Calendar;
import java.util.Random;

public class ClickSourceWithWatermark implements ParallelSourceFunction<Event> {
    // 声明标志位
    private Boolean running = true;

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        //随机生成数据
        Random random = new Random();
        //随机范围
        String[] users = {"令狐冲", "依琳", "任盈盈", "莫大", "风清扬"};

        String[] urls = {"./home", "./cat", "./pay", "./info"};
        //循环生成数据
        while (running) {
            //生成数据
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            Event event = new Event(user, url, Calendar.getInstance().getTimeInMillis());

            //发送数据
            ctx.collect(event);

            //发送水位线
            ctx.emitWatermark(new Watermark(event.getTimestamp() - 1L));
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}