package com.turing.java.flink20.function;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * public static Table report(Table transactions) {
 *     return transactions.select(
 *             $("account_id"),
 *             call(MyFloor.class, $("transaction_time")).as("log_ts"),
 *             $("amount"))
 *         .groupBy($("account_id"), $("log_ts"))
 *         .select(
 *             $("account_id"),
 *             $("log_ts"),
 *             $("amount").sum().as("amount"));
 * }
 * */
public class MyFloor extends ScalarFunction {

    public @DataTypeHint("TIMESTAMP(3)") LocalDateTime eval(
            @DataTypeHint("TIMESTAMP(3)") LocalDateTime timestamp) {

        return timestamp.truncatedTo(ChronoUnit.HOURS);
    }

}