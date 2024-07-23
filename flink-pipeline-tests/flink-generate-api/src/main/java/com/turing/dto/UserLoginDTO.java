package com.turing.dto;

import io.swagger.v3.oas.annotations.media.Schema;

import java.io.Serializable;

/**
 * @descr: 用户登录时传递的数据模型
 * */
@Schema(description = "用户登录时传递的数据模型")
public class UserLoginDTO implements Serializable {

    @Schema(description = "用户名", example = "Tony")
    private String username;

    @Schema(description = "密码", example = "Turing@123")
    private String password;

}
