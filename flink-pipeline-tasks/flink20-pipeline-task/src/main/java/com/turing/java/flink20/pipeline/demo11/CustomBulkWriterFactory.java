package com.turing.java.flink20.pipeline.demo11;

import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;

import java.io.IOException;

/**
 * 自定义列模式的文件压缩算法
 * */
public class CustomBulkWriterFactory implements BulkWriter.Factory<String> {

    @Override
    public BulkWriter<String> create(FSDataOutputStream out) throws IOException {
        GzipCompressorOutputStream gzipOutput = new GzipCompressorOutputStream(out);

        return new BulkWriter<String>() {
            @Override
            public void addElement(String element) throws IOException {
                gzipOutput.write(element.getBytes());
            }

            @Override
            public void flush() throws IOException {
                gzipOutput.flush();
            }

            @Override
            public void finish() throws IOException {
                gzipOutput.close();
            }
        };
    }
}