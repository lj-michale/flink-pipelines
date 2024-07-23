package com.turing.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

/**
 * @descr 信用卡实体
 * @author Tony
 * */
@Data
@EqualsAndHashCode(callSuper = true)
@Accessors(chain = true)
@TableName("t_card_puncher")
public class CardPuncher extends BaseEntity {

    /**
     * 打卡人姓名
     */
    private String name;

}

