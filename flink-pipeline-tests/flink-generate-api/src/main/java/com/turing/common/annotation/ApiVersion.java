package com.turing.common.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @descr API版本声明注解
 * */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE,ElementType.FIELD, ElementType.METHOD})
public @interface ApiVersion {

    /**
     * 具体的版本号
     * */
    String value();

}
