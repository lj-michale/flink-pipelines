package com.turing.java.flink19.pipeline.demo1.function;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * 标量函数（Scalar Functions）
 * */
// TODO 自定义函数的实现类
public class MyHashFunction extends ScalarFunction {
    // 接受任意类型的输入 返回int类型输出
    public int eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object o){
        return o.hashCode();
    }
}