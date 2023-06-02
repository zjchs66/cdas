package com.xjj.flink.function.cdas.enums;

/**
 * @author zhoujuncheng
 * @date 2022/5/13
 */
public enum SinkTypeEnums {

    CLICKHOUSE("clickhouse"),KINGBASE("kingbase"),MYSQL("mysql"),MONGO("mongo"),ORACLE("oracle"),ELASTICSEARCH("elasticsearch");

    private String type;

    SinkTypeEnums(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public static boolean contains(String type) {
        for (SinkTypeEnums typeEnums : SinkTypeEnums.values()) {
            if (typeEnums.getType().equals(type)) {
                return true;
            }
        }
        return false;
    }
}
