package com.turing.controller.user;

import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Tag(name = "用户操作接口", description = "用户操作相关操作")
@RequestMapping("/api/web/user")
public class UserController {


}
