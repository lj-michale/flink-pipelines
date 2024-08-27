package com.turing.java.flink20.pipeline.demo11;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.impl.StreamFormatAdapter;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;


/**
 * 描述：flink集成FileSource & forBulkFileFormat使用 & 流模式
 * BulkFormat：从文件中一次读取一批记录。 它虽然是最 “底层” 的格式实现，但是提供了优化实现的最大灵活性。
 */
public class FileSourceBulkStreamingJob {

    public static void main(String[] args) throws Exception {

        //创建 批量读取文件的格式函数，其实底层还是通过对单行文件读取
        BulkFormat<String, FileSourceSplit> bulkFormat = new StreamFormatAdapter<>(new TextLineInputFormat());

        // 创建 FileSource
        FileSource<String> fileSource = FileSource.
                forBulkFileFormat(bulkFormat, new Path("D:\\flink\\file_source.txt"))
                //放开注释则使用流模式,每隔5分钟检查是否有新文件，否则默认使用批模式
//                .monitorContinuously(Duration.ofMillis(5))
                .build();

        // 创建 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 添加 FileSource 到数据流
        env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "FileSource").print();

        // 执行任务
        env.execute("FileSourceBulkStreamingJob");
    }
}

