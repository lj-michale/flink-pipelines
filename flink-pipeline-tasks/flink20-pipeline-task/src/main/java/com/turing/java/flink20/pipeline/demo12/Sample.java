//package com.turing.java.flink20.pipeline.demo12;
//
//
//import org.apache.flink.table.gateway.api.results.ResultSet;
//
//
//
//
//public class Sample {
//    public static void main(String[] args) throws Exception {
//        try (Connection connection = DriverManager.getConnection("jdbc:flink://localhost:8083")) {
//            try (Statement statement = connection.createStatement()) {
//                statement.execute("CREATE TABLE T(\n" +
//                        "  a INT,\n" +
//                        "  b VARCHAR(10)\n" +
//                        ") WITH (\n" +
//                        "  'connector' = 'filesystem',\n" +
//                        "  'path' = 'file:///tmp/T.csv',\n" +
//                        "  'format' = 'csv'\n" +
//                        ")");
//                statement.execute("INSERT INTO T VALUES (1, 'Hi'), (2, 'Hello')");
//                try (ResultSet rs = statement.executeQuery("SELECT * FROM T")) {
//                    while (rs.next()) {
//                        System.out.println(rs.getInt(1) + ", " + rs.getString(2));
//                    }
//                }
//            }
//        }
//    }
//}