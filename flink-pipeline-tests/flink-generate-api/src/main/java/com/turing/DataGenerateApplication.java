package com.turing;

import lombok.extern.slf4j.Slf4j;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.env.ConfigurableEnvironment;

/**
 * @descr:
 * @author: Tony
 * */
@Slf4j
@MapperScan("com.turing.mapper")
@SpringBootApplication
public class DataGenerateApplication {

    public static void main(String[] args) {
        ConfigurableEnvironment env = SpringApplication
                .run(DataGenerateApplication.class, args)
                .getEnvironment();
        String applicationName = env.getProperty("spring.application.name");
        String serverPort = env.getProperty("server.port");
        log.info("\n----------------------------------------------------------\n\t" +
                 "Application: '{}' is running Success! \n\t" +
                 "Local URL: \thttp://localhost:{}\n\t" +
                 "Document:\thttp://localhost:{}/doc.html\n" +
                 "----------------------------------------------------------",
                applicationName, serverPort, serverPort);
    }

}
