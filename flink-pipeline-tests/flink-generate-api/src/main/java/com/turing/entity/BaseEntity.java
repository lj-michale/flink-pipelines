package com.turing.entity;

import lombok.Data;
import java.io.Serializable;
import java.util.Date;

/**
 * @descr: 基类实体
 * @author: Tony
 * */
@Data
public class BaseEntity {

    private Integer id;

    private Date createTime;

    private Integer creator;

    private Date updateTime;

    private Integer updator;
}