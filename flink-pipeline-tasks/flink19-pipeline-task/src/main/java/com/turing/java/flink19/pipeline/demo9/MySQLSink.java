//package com.turing.java.flink19.pipeline.demo9;
//
//import org.apache.flink.api.common.functions.RuntimeContext;
//import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
//
//import java.sql.Connection;
//
//public class MySQLSink extends RichSinkFunction<TemperatureHumidityRecord> {
//
//    private transient Connection connection;
//    private final String url;
//    private final String user;
//    private final String password;
//
//    public MySQLSink(String url, String user, String password) {
//        this.url = url;
//        this.user = user;
//        this.password = password;
//    }
//
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        super.open(parameters);
//        // 初始化数据库连接
//        Class.forName("com.mysql.jdbc.Driver");
//        connection = DriverManager.getConnection(url, user, password);
//    }
//
//    @Override
//    public void invoke(TemperatureHumidityRecord record, Context context) throws Exception {
//        String sql = "INSERT INTO iot_data(device_id, temperature, humidity, timestamp) VALUES(?,?,?,?)";
//        try (PreparedStatement statement = connection.prepareStatement(sql)) {
//            statement.setInt(1, record.getDeviceId());
//            statement.setDouble(2, record.getTemperature());
//            statement.setDouble(3, record.getHumidity());
//            statement.setTimestamp(4, new Timestamp(record.getTimestamp().getTime()));
//            statement.executeUpdate();
//        }
//    }
//
//    @Override
//    public void close() throws Exception {
//        if (connection != null) {
//            connection.close();
//        }
//        super.close();
//    }
//}