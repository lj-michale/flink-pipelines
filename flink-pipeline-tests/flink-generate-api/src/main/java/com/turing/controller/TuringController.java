package com.turing.controller;


import com.turing.flink.convention.springboot.starter.result.Result;
import com.turing.flink.web.springboot.starter.Results;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Tag(name = "测试列表", description = "测试相关操作")
@RequestMapping("/api/web")
public class TuringController {

    @Operation(summary = "根据订单号查询票据信息", description = "根据订单号查询票据信息")
    @GetMapping("/queryTicketOrderByOrderSn")
    public Result<String> queryTicketOrderByOrderSn(@RequestParam(value = "orderSn") String orderSn) {
        String sre = "测试成功" + orderSn;
        return Results.success(sre);
    }

}
