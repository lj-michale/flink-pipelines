package com.turing.java.flink20.pipeline.demo9;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.api.connector.source.Source;
import java.util.Random;

public class ClickSource implements SourceFunction<Event> {

    //声明一个标志位
    private boolean running = true;

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        //随机生成数据
        Random random = new Random();
        //定义字段选取的数据集
        String[] users = {"Mary", "Alice", "Bobo", "lucy"};
        String[] urls = {"./home", "./cart", "./prod", "./order"};

        //循环生成数据
        while (running){
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            long timestamp = System.currentTimeMillis();
            sourceContext.collect(new Event(user,url,timestamp));

            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}