package com.turing.service.impl.user;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.turing.service.user.UserService;
import io.swagger.v3.oas.annotations.servers.Server;

@DS(value = "turing")
@Server
public class UserServiceImpl implements UserService {

}
