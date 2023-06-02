package com.xjj.flink.function.cdas.table;


import com.xjj.flink.function.cdas.entity.Operator;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author zhoujuncheng
 * @date 2022/6/14
 */
public class OracleSink extends JdbcSink {

    public OracleSink(String url, String username, String password, String database, int batchSize, long interval) {
        super(url, username, password, database, batchSize, interval);
        this.drivername = "oracle.jdbc.driver.OracleDriver";
    }

    @Override
    String getCreateSql(Operator row, Map<String, String> cols) throws Exception {
        String s = "select * from all_tab_comments where owner = '" + this.database.toUpperCase() + "' and table_name = '" + row.getTablename().toUpperCase() + "'";
        Statement st = connection.createStatement();
        ResultSet rs = st.executeQuery(s);

        if (rs.next()) {
            rs.close();
            st.close();
            return null;
        }

        StringBuilder sql = new StringBuilder("create table " + this.database + ".");
        sql.append(row.getTablename() + "(");

        String columns = "";
        int i = 0;
        for (Map.Entry<String, String> entry : cols.entrySet()) {
            if (i == cols.size() - 1) {
                columns += entry.getKey() + " " + cols.get(entry.getKey());
            } else {
                columns += entry.getKey() + " " + cols.get(entry.getKey()) + ",";
            }
            i++;
        }

        sql.append(columns);
        String prs = commonPrSql(new ArrayList<>(row.getPr().keySet()));
        sql.append(",dbz_op_time timestamp,dbz_op_type varchar(10)");
        if (!prs.equals("")) {
            String index = this.database + "_" + row.getTablename() + "_pk";
            sql.append(",constraint " + index + " primary key");
            sql.append(prs);
        }
        sql.append(")");
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
                return "boolean";
            case "int8":
                return "number(8)";
            case "int16":
                return "number(16)";
            case "int32":
                return "number(32)";
            case "int64":
                return "number(64)";
            case "float32":
                return "number(32,8)";
            case "float64":
            case "double":
                return "number(64,8)";
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
    public String getAddColumnsSql(String tableName, String column, String type) throws Exception {
        String s = "select * from all_col_comments where owner = ? and table_name = ? and column_name = ?";
        PreparedStatement ps = connection.prepareStatement(s);
        ps.setObject(1, this.database.toUpperCase());
        ps.setObject(2, tableName.toUpperCase());
        ps.setObject(3, column.toUpperCase());
        ResultSet rs = ps.executeQuery();
        if (rs.next()) {
            rs.close();
            ps.close();
            return null;
        }
        StringBuilder sql = new StringBuilder("alter table  " + this.database + ".");
        sql.append(tableName);
        sql.append(" add   ");
        sql.append(column + " ");
        sql.append(type);
        return sql.toString();
    }

    @Override
    public void preparedStatement(PreparedStatement preparedStatement, Operator sqlOperator, Map<String, String> columns) throws Exception {
        System.out.println(sqlOperator.toString());
        if (sqlOperator.getPr() != null && sqlOperator.getPr().size() > 0) {
            int i = 1;
            for (Map.Entry<String, Object> entry : sqlOperator.getPr().entrySet()) {
                preparedStatement.setObject(i, entry.getValue());
                i++;
            }
            preparedStatement.setTimestamp(i++, new Timestamp(sqlOperator.getOpts()));
            preparedStatement.setString(i++, sqlOperator.getOpType());
            for (Map.Entry<String, String> entry : columns.entrySet()) {
                preparedStatement.setObject(i, sqlOperator.getColumnsValue().get(entry.getKey()));
                i++;
            }
            preparedStatement.setTimestamp(i++, new Timestamp(sqlOperator.getOpts()));
            preparedStatement.setString(i++, sqlOperator.getOpType());
            for (Map.Entry<String, String> entry : columns.entrySet()) {
                if (sqlOperator.getPr().containsKey(entry.getKey())) {
                    continue;
                }
                preparedStatement.setObject(i, sqlOperator.getColumnsValue().get(entry.getKey()));
                i++;
            }
        } else {
            preparedStatement.setTimestamp(1, new Timestamp(sqlOperator.getOpts()));
            preparedStatement.setString(2, sqlOperator.getOpType());
            int i = 3;
            for (Map.Entry<String, String> entry : columns.entrySet()) {
                preparedStatement.setObject(i, sqlOperator.getColumnsValue().get(entry.getKey()));
                i++;
            }
        }
        preparedStatement.addBatch();
    }

    @Override
    public String getInsertSql(Operator row, Map<String, String> cols) {
        StringBuilder sb = new StringBuilder();
        String tableName = row.getTablename();

        List<String> prs = new ArrayList<>(row.getPr().keySet());
        if (prs != null && prs.size() > 0) {
            String pr = "(";
            int i = 0;
            for (Map.Entry<String, Object> entry : row.getPr().entrySet()) {
                if (i == prs.size() - 1) {
                    pr += tableName + "." + entry.getKey() + "= ?)";
                } else {
                    pr += tableName + "." + entry.getKey() + "= ?,";
                }
                i++;
            }
            sb.append("merge into " + this.database + "." + tableName + " USING dual on" + pr);
            sb.append(" when not matched then ");
            String columns = "(dbz_op_time,dbz_op_type,";
            String update = "";
            String vals = "(?,?,";
            int j = 0;

            for (Map.Entry<String, String> entry : cols.entrySet()) {
                if (j == cols.size() - 1) {
                    columns += entry.getKey() + ")";
                    vals += "?)";
                } else {
                    columns += entry.getKey() + ",";
                    vals += "?,";
                }
                j++;
            }

            int p = 0;
            List<String> k = cols.keySet().stream().filter(x -> !prs.contains(x)).collect(Collectors.toList());
            for (String col : k) {
                if (p == k.size() - 1) {
                    update += col + "=" + "?";
                } else {
                    update += col + "=" + "?,";
                }
                p++;
            }

            sb.append(" insert " + columns + " values " + vals);
            sb.append(" when matched then ");
            sb.append(" update set dbz_op_time = ?,dbz_op_type = ?," + update);
            return sb.toString();
        } else {
            return super.getInsertSql(row, cols);
        }

    }
}
