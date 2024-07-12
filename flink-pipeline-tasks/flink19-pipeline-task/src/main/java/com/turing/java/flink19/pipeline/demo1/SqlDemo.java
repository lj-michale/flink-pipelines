package com.turing.java.flink19.pipeline.demo1;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import static org.apache.flink.table.api.Expressions.$;

public class SqlDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO 1.创建表环境
        // 1.1 写法1：
//        EnvironmentSettings es = EnvironmentSettings.newInstance()
//                .inStreamingMode()
//                .build();
//        TableEnvironment table_env = TableEnvironment.create(es);

        // 1.2 写法2：
        StreamTableEnvironment table_env = StreamTableEnvironment.create(env);

        // TODO 2. 创建表
        table_env.executeSql("CREATE TABLE source ( \n" +
                "    id INT, \n" +
                "    ts BIGINT, \n" +
                "    vc INT\n" +
                ") WITH ( \n" +
                "    'connector' = 'datagen', \n" +
                "    'rows-per-second'='1', \n" +
                "    'fields.id.kind'='random', \n" +
                "    'fields.id.min'='1', \n" +
                "    'fields.id.max'='10', \n" +
                "    'fields.ts.kind'='sequence', \n" +
                "    'fields.ts.start'='1', \n" +
                "    'fields.ts.end'='1000000', \n" +
                "    'fields.vc.kind'='random', \n" +
                "    'fields.vc.min'='1', \n" +
                "    'fields.vc.max'='100'\n" +
                ");\n");
        table_env.executeSql("CREATE TABLE sink (\n" +
                "    id INT, \n" +
                "    sum_vc INT \n" +
                ") WITH (\n" +
                "'connector' = 'print'\n" +
                ");\n");

        // TODO 3. 执行查询 查询的结果也是一张表
        // 3.1 使用sql进行查询
        Table table = table_env.sqlQuery("select id,sum(vc) as sum_vc from source where id > 5 group by id;");
        // 把 table 对象注册成为表名
        table_env.createTemporaryView("tmp",table);
//        table_env.sqlQuery("select * from tmp;");
        // 3.2 用table api查询
//        Table source = table_env.from("source");
//        Table result = source
//                .where($("id").isGreater(5))
//                .groupBy($("id"))
//                .aggregate($("vc").sum().as("sum_vc"))
//                .select($("id"), $("sum_vc"));

        // TODO 4. 输出表
        // 4.1 sql用法
        table_env.executeSql("insert into sink select * from tmp");
        // 4.2 table api
//        result.executeInsert("sink");
    }
}