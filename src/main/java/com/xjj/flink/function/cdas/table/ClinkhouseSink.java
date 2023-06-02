package com.xjj.flink.function.cdas.table;


import com.xjj.flink.function.cdas.entity.Operator;
import com.xjj.flink.function.cdas.util.CommonConstant;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author zhoujuncheng
 * @date 2022/4/28
 */
@Slf4j
public class ClinkhouseSink extends JdbcSink {


    List<String> mergeKeys = new ArrayList();


    public ClinkhouseSink(String url, String username, String password, String database, int batchSize, long interval, String mergeKey) {
        super(url, username, password, database, batchSize, interval);
        this.drivername = "ru.yandex.clickhouse.ClickHouseDriver";
        if (StringUtils.isNotBlank(mergeKey)) {
            mergeKeys = Arrays.asList(mergeKey.split(","));
        }
    }

    @Override
    String getCreateSql(Operator row, Map<String, String> cols) {

        StringBuilder sql = commonCreateSql(row, cols);

        sql.append(",dbz_op_time DateTime64(0),dbz_op_type String");

        sql.append(")");

        List<String> keys = new ArrayList<>(row.getPr().keySet());
        keys.addAll(mergeKeys);

        String ps = commonPrSql(keys);
        if(!CommonConstant.appendOpKind){
            if (!ps.equals("")) {
                sql.append("ENGINE = ReplacingMergeTree(dbz_op_time) order by ");
                sql.append(ps);
            } else {
                sql.append("ENGINE = MergeTree() order by (dbz_op_time)");
            }
        }else {
            if (!ps.equals("")) {
                String kks = "";
                for(String key : keys){
                    kks += "`" + key + "`" + ",";
                }
                sql.append("ENGINE = MergeTree() order by (" + kks + "dbz_op_time)");
                //sql.append(ps);
            } else {
                sql.append("ENGINE = MergeTree() order by (dbz_op_time)");
            }
        }

        sql.append(" SETTINGS index_granularity = 8192");
        return sql.toString();
    }

    @Override
    Map<String, String> getColumnsInfo(Operator sqlOperator) throws Exception {
        Map<String, String> colums = sqlOperator.getColumnsType();
        for (String key : colums.keySet()) {
            if (sqlOperator.getPr().containsKey(key) || mergeKeys.contains(key)) {
                colums.put(key, columntypetransfer(colums.get(key), true));
            } else {
                colums.put(key, columntypetransfer(colums.get(key), false));
            }
        }
        return colums;
    }


    @Override
    String columntypetransfer(String type, Boolean flag) throws Exception {
        String result = type;
        if (!flag) {
            return "Nullable(" + result + ")";
        }
        return result;
    }


    @Override
    public String columnTypeMapper(String type, Map<String, Object> param) throws Exception {
        switch (type) {
            case "boolean":
                return "UInt8";
            case "int8":
                return "Int8";
            case "int16":
                return "Int16";
            case "int32":
                return "Int32";
            case "int64":
                return "Int64";
            case "float32":
                return "float";
            case "float64":
            case "double":
                return "double";
            case "io.debezium.data.Json":
                return "json";
            case "io.debezium.data.VariableScaleDecimal":
            case "com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Decimal":
                return "Decimal64(5)";
            case "string":
            case "io.debezium.data.Xml":
            case "io.debezium.time.ZonedTimestamp":
            case "bytes":
                return "String";
            case "io.debezium.time.Date":
            case "com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Date":
                return "Date";
            case "io.debezium.time.Time":
            case "io.debezium.time.MicroTime":
            case "io.debezium.time.NanoTime":
            case "io.debezium.time.Timestamp":
            case "io.debezium.time.MicroTimestamp":
            case "io.debezium.time.NanoTimestamp":
            case "com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Timestamp":
                return "DateTime64";
            case "io.debezium.time.ZonedTime":
            default:
                return type;
        }
    }

    public String convertDecimal(String type, Map<String, Object> param) {
        if (type.equals("com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Decimal")) {
            String precision = (String) param.getOrDefault("precision", "32");
            String scale = (String) param.getOrDefault("scale", "0");
            return "Decimal" + precision + "(" + scale + ")";
        } else {
            return "Decimal64(5)";
        }
    }

}
