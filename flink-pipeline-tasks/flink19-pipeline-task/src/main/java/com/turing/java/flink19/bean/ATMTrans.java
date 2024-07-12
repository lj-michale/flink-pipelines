package com.turing.java.flink19.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.sql.Timestamp;

@Data
@AllArgsConstructor
public class ATMTrans implements Serializable {
    private String account_id;
    private String transaction_id;
    private int amount;
    private String atm;
    private Double lat;
    private Double lon;
    private Timestamp ts;
}