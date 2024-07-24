package com.turing.controller;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.turing.entity.Company;
import com.turing.flink.convention.springboot.starter.result.Result;
import com.turing.flink.log.springboot.starter.annotation.ILog;
import com.turing.flink.web.springboot.starter.Results;
import com.turing.mapper.CompanyMapper;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.Resource;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@Tag(name = "测试列表", description = "测试相关操作")
@RequestMapping("/api/web")
public class TuringController {

    @Resource
    private CompanyMapper companyMapper;

    @ILog
    @Operation(summary = "根据订单号查询票据信息", description = "根据订单号查询票据信息")
    @GetMapping("/queryTicketOrderByOrderSn")
    public Result<List<Company>> queryTicketOrderByOrderSn(@RequestParam(value = "orderSn") String orderSn) {
        String sre = "测试成功" + orderSn;
        QueryWrapper<Company> query1 = new QueryWrapper<>();
        query1.eq("contact","Tony");
        List<Company> companyList = companyMapper.selectList(query1);

        return Results.success(companyList);
    }

}
