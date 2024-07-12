package com.turing.java.flink19.pipeline.demo3;

/**
 * 、表函数（Table Functions）
 * */
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

@FunctionHint(output = @DataTypeHint("ROW<word STRING,length INT>"))
public class MySplitFunction extends TableFunction<Row> {
    // 返回是 void ，用 collect 方法输出
    public void eval(String str){
        for (String word : str.split(" ")) {
            collect(Row.of(word,word.length()));
        }
    }
}