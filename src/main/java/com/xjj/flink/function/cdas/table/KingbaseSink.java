package com.xjj.flink.function.cdas.table;


import com.xjj.flink.function.cdas.entity.Operator;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author zhoujuncheng
 * @date 2022/4/29
 */
public class KingbaseSink extends JdbcSink {
    public KingbaseSink(String url, String username, String password, String database, int batchSize, long interval, String driver) {
        super(url, username, password, database, batchSize, interval);
        if (StringUtils.isNotBlank(driver)) {
            this.drivername = driver;
        } else {
            this.drivername = com.kingbase8.Driver.class.getName();
        }
    }

    @Override
    String getCreateSql(Operator row, Map<String, String> cols) {
        StringBuilder sql = commonCreateSql(row, cols);
        String prs = commonPrSql(new ArrayList<>(row.getPr().keySet()));
        sql.append(",dbz_op_time timestamp,dbz_op_type string");
        if (!prs.equals("")) {
            sql.append(",primary key ");
            sql.append(prs);
        }
        sql.append(")");
        System.out.println(sql.toString());
        return sql.toString();
    }

    @Override
    String columntypetransfer(String type, Boolean flag) throws Exception {
        String result = type;
        if (flag) {
            return result + " not null";
        }
        return result;
    }

    @Override
    public String columnTypeMapper(String type, Map<String, Object> param) throws Exception {
        switch (type) {
            case "boolean":
                return "boolean";
            case "int8":
                return "integer";
            case "int16":
                return "integer";
            case "int32":
                return "bigint";
            case "int64":
                return "bigint";
            case "float32":
                return "float";
            case "float64":
            case "double":
                return "double";
            case "io.debezium.data.VariableScaleDecimal":
                return "decimal";
            case "string":
            case "io.debezium.data.Xml":
            case "io.debezium.time.ZonedTimestamp":
            case "bytes":
                return "varchar";
            case "io.debezium.time.Date":
            case "org.apache.kafka.connect.data.Date":
            case "com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Date":
                return "date";
            case "io.debezium.time.Time":
            case "io.debezium.time.MicroTime":
            case "io.debezium.time.NanoTime":
            case "io.debezium.time.Timestamp":
            case "io.debezium.time.MicroTimestamp":
            case "io.debezium.time.NanoTimestamp":
            case "org.apache.kafka.connect.data.Timestamp":
            case "com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Timestamp":
                return "timestamp";
            case "io.debezium.time.ZonedTime":
            default:
                throw new Exception("不支持该字段类型：" + type);
        }
    }

    @Override
    public String getInsertSql(Operator row, Map<String, String> cols) {
        String s = super.getInsertSql(row, cols);
        StringBuilder sql = new StringBuilder();
        sql.append(s);

        List<String> prs = new ArrayList<>(row.getPr().keySet());
        if (prs != null && prs.size() > 0) {
            int j = 0;
            String prs1 = "on CONFLICT(";
            for (String pri : prs) {
                if (j == prs.size() - 1) {
                    prs1 += "\"" + pri + "\"" + ")";
                } else {
                    prs1 += "\"" + pri + "\"" + ",";
                }
                j++;
            }
            prs1 += " do update set ";
            int k = 0;

            for (Map.Entry<String, String> entry : cols.entrySet()) {


                prs1 += "\"" + entry.getKey() + "\"" + "=?,";

            }

            prs1 += "\"dbz_op_time\"" + "= ?,";
            prs1 += "\"dbz_op_type\"" + "= ?";

            sql.append(" " + prs1);
        }
        return sql.toString();
    }
}
