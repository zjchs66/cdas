package com.xjj.flink.function.cdas.entity;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class Operator {

    String db;
    String tablename;
    String opType;
    Map<String, Object> pr = new HashMap<>();
    Map<String, String> columnsType = new HashMap<>();
    Map<String, Object> columnsValue = new HashMap<>();
    //    Map<String,String> cusColumnsType = new HashMap<>();
//    Map<String,Object> cusColumnsValue = new HashMap<>();
    Long opts;

}
