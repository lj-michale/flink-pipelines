package com.turing.controller.authority;

import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Tag(name = "权限操作接口", description = "权限操作相关操作")
@RequestMapping("/api/web/authority")
public class AuthorityController {

}
