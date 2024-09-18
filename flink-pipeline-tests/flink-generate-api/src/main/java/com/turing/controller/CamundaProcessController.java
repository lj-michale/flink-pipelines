package com.turing.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @descr 流程控制演示
 * @author Tony
 * @date 2024-09
 * */
@RestController
@Tag(name = "流程控制接口列表", description = "流程控制接口相关操作")
@RequestMapping("/api/web/camunda")
public class CamundaProcessController {

    @PostMapping("/user/{processKey}")
    @Operation(method = "启动流程", tags = "流程管理", summary = "开启流程")
    public String startFlow(@PathVariable(value = "processKey") String processKey) {
        return "";
    }


}
