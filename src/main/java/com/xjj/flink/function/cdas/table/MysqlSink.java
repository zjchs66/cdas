package com.xjj.flink.function.cdas.table;


import com.xjj.flink.function.cdas.entity.Operator;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author zhoujuncheng
 * @date 2022/4/29
 */
public class MysqlSink extends JdbcSink {


    public MysqlSink(String url, String username, String password, String database, int batchSize, long interval) {
        super(url, username, password, database, batchSize, interval);
        this.drivername = "com.mysql.jdbc.Driver";
    }

    @Override
    String getCreateSql(Operator row, Map<String, String> cols) {
        StringBuilder sql = commonCreateSql(row, cols);
        String prs = commonPrSql(new ArrayList<>(row.getPr().keySet()));
        sql.append(",dbz_op_time timestamp,dbz_op_type varchar(10)");
        if (!prs.equals("")) {
            sql.append(",primary key ");
            sql.append(prs);
        }
        sql.append(")");
        sql.append("ENGINE=InnoDB DEFAULT CHARSET=utf8");
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
    public String columnTypeMapper(String type,Map<String,Object> param) throws Exception {
        switch (type) {
            case "boolean":
                return "tinyint(1)";
            case "int8":
                return "int";
            case "int16":
                return "int";
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
                return "varchar(500)";
            case "io.debezium.time.Date":
            case "com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Date":
                return "date";
            case "io.debezium.time.Time":
            case "io.debezium.time.MicroTime":
            case "io.debezium.time.NanoTime":
            case "io.debezium.time.Timestamp":
            case "io.debezium.time.MicroTimestamp":
            case "io.debezium.time.NanoTimestamp":
            case "com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Timestamp":
                return "timestamp";
            case "io.debezium.time.ZonedTime":
            default:
                throw new Exception("不支持该字段类型：" + type);
        }
    }

    @Override
    public void preparedStatement(PreparedStatement preparedStatement, Operator sqlOperator, Map<String, String> columns) throws Exception {
        preparedStatement.setTimestamp(1, new Timestamp(sqlOperator.getOpts()));
        preparedStatement.setString(2, sqlOperator.getOpType());
        int i = 3;
        for (Map.Entry<String, String> entry : columns.entrySet()) {
            preparedStatement.setObject(i, sqlOperator.getColumnsValue().get(entry.getKey()));
            i++;
        }

        if (sqlOperator.getPr() != null && sqlOperator.getPr().size() > 0) {
            for (Map.Entry<String, String> entry : columns.entrySet()) {
                preparedStatement.setObject(i, sqlOperator.getColumnsValue().get(entry.getKey()));
                i++;
            }
            preparedStatement.setObject(i++, new Timestamp(sqlOperator.getOpts()));
            preparedStatement.setObject(i, sqlOperator.getOpType());
        }

        preparedStatement.addBatch();

    }

    @Override
    public String getInsertSql(Operator row, Map<String, String> cols) {
        String s = super.getInsertSql(row, cols);
        StringBuilder sql = new StringBuilder();
        sql.append(s);

        List<String> prs = new ArrayList<>(row.getPr().keySet());
        if (prs != null && prs.size() > 0) {

            String prs1 = " ON DUPLICATE KEY UPDATE ";
            int k = 0;

            for (Map.Entry<String, String> entry : cols.entrySet()) {

                prs1 += entry.getKey() + "= ?,";

            }
            prs1 += "dbz_op_time = ?,dbz_op_type = ?";
            sql.append(" " + prs1);
        }
        return sql.toString();
    }
}
