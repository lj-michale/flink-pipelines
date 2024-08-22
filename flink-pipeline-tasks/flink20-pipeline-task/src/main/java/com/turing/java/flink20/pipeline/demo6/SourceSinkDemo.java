package com.turing.java.flink20.pipeline.demo6;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class SourceSinkDemo {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        RandomStudentSource randomStudentSource = new RandomStudentSource();
        DataStreamSource<Student> dataStreamSource = env
                .addSource(randomStudentSource);


    }

    private static class RandomStudentSource implements SourceFunction<Student> {

        private Random rnd = new Random();
        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Student> ctx) throws Exception {
            while (isRunning) {
                Student student = new Student();
                student.setName("name-" + rnd.nextInt(5));
                student.setScore(rnd.nextInt(20));
                ctx.collect(student);
                Thread.sleep(100L);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    public static class Student {
        private String name;
        private Integer score;

        public Student() {
        }

        public Student(String name, Integer score) {
            this.name = name;
            this.score = score;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getScore() {
            return score;
        }

        public void setScore(Integer score) {
            this.score = score;
        }

        @Override
        public String toString() {
            return "Student{" +
                    "name='" + name + '\'' +
                    ", score=" + score +
                    '}';
        }
    }
}
