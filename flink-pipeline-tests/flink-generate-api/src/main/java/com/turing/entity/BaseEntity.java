package com.turing.entity;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import java.io.Serializable;
import java.util.Date;

/**
 * @descr: 基类实体
 * @author: Tony
 * */
@Data
@Schema(description = "基础实体")
public class BaseEntity implements Serializable {

    @Schema(description = "ID", example = "DTG27134715")
    private Integer id;

//    @Schema(description = "创建日期", example = "")
//    private Date createTime;

    @Schema(description = "创建人", example = "")
    private Integer creator;

//    @Schema(description = "更新日期", example = "")
//    private Date updateTime;

    @Schema(description = "更新人", example = "")
    private Integer updator;
}