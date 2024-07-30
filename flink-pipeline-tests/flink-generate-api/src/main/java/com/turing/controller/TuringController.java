package com.turing.controller;

import com.baomidou.dynamic.datasource.annotation.DS;
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

import java.util.*;

@RestController
@Tag(name = "测试接口列表", description = "测试接口相关操作")
@RequestMapping("/api/web/turing")
public class TuringController {

    @Resource
    private CompanyMapper companyMapper;

    @ILog
    @Operation(summary = "根据联系人查询公司信息", description = "根据联系人查询公司信息")
    @GetMapping("/queryCompanyByContact")
    @DS(value = "turing")
    public Result<List<Company>> queryTicketOrderByOrderSn(@RequestParam(value = "contact") String contact) {
        QueryWrapper<Company> query = new QueryWrapper<>();
        query.eq("contact",contact);
        List<Company> companyList = companyMapper.selectList(query);

        return Results.success(companyList);
    }

    @ILog
    @Operation(summary = "查询图表1数据信息", description = "查询图表1数据信息")
    @GetMapping("/queryChartOne")
    public Result<Map<String,Object>> queryChartOne() {
        //返回前端的数据
        List<Map<String,Object>> mapList = new ArrayList<>();
        //循环
        List<String> xAxis = new ArrayList<>();
        List<Integer> yAxis = new ArrayList<>();
        List<Integer> yAxis2 = new ArrayList<>();
        String[] clothes = {"数字", "宽带", "OTT", "移网", "固话", "集团"};
        xAxis.addAll(Arrays.asList(clothes));
        int[] number = {5, 20, 36, 10, 10, 20};
        for (int num : number) {
            yAxis.add(num);
        }
        int[] number2 = {5, 30, 66, 10, 20, 80};
        for (int num : number2) {
            yAxis2.add(num);
        }
        Map<String,Object> map = new HashMap<>();
        map.put("xAxis",xAxis);
        map.put("yAxis",yAxis);
        map.put("yAxis2",yAxis2);

        return Results.success(map);
    }

}
