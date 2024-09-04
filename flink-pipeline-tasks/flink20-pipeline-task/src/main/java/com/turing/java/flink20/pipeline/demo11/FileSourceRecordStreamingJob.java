package com.turing.java.flink20.pipeline.demo11;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * 描述:
 * flink集成FileSource & forRecordStreamFormat使用 & 流模式
 * StreamFormat：从文件流中读取文件内容。它是最简单的格式实现，
 * 并且提供了许多拆箱即用的特性（如 Checkpoint 逻辑），
 * 但是限制了可应用的优化（例如对象重用，批处理等等）。
 */
public class FileSourceRecordStreamingJob {

    public static void main(String[] args) throws Exception {

        // 创建 需要读取的文件路径Path
        Path path = new Path("D:\\flink\\file_source.txt");

        // 创建 读取文件的格式函数
        TextLineInputFormat textLineInputFormat = new TextLineInputFormat();

        // 创建 FileSource
        FileSource<String> fileSource = FileSource.
                forRecordStreamFormat(textLineInputFormat, path)
                //放开注释则使用流模式，每隔5分钟检查是否有新文件否则默认使用批模式
//                .monitorContinuously(Duration.ofMillis(5))
                .build();

        // 创建 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 添加 FileSource 到数据流
        env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "FileSource").print();

        // 执行任务
        env.execute("FileSourceRecordStreamingJob");
    }
}

