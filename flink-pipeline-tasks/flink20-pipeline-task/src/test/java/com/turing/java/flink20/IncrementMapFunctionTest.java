package com.turing.java.flink20;

import com.turing.java.flink20.function.IncrementMapFunction;
import org.junit.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class IncrementMapFunctionTest {

    @Test
    public void testIncrement() throws Exception {
        // instantiate your function
        IncrementMapFunction incrementer = new IncrementMapFunction();
        // call the methods that you have implemented
        assertEquals(3L, incrementer.map(2L));
    }

}