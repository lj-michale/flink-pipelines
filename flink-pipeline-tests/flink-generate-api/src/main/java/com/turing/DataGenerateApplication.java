package com.turing;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @descr:
 * @author: Tony
 * */
@MapperScan("com.turing.mapper.*")
@SpringBootApplication
public class DataGenerateApplication {

    public static void main(String[] args) {
        SpringApplication.run(DataGenerateApplication.class, args);
    }
}
