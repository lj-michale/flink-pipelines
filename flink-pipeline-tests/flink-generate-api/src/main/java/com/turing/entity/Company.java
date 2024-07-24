package com.turing.entity;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableName;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import java.util.Date;

@Data
//@EqualsAndHashCode(callSuper = true)
@Accessors(chain = true)
@TableName("company")
public class Company {

    @Schema(description = "ID", example = "DTG27134715")
    private Integer id;

    private String name;

    private String contact;

    @TableField("contact_type")
    private String contactType;

    @TableField("create_time")
    private Date createTime;

    @TableField("update_time")
    private Date updateTime;

    @TableField("delete_time")
    private Date deleteTime;

    private int removed;

}
