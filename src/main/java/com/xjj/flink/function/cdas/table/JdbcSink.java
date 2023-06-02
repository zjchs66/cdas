package com.xjj.flink.function.cdas.table;

import com.xjj.flink.function.cdas.entity.Operator;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import ru.yandex.clickhouse.except.ClickHouseException;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zhoujuncheng
 * @date 2022/4/28
 */
@Slf4j
abstract class JdbcSink extends AbstractCdasSinkFunction<String> {

    public Map<String, PreparedStatement> preparedStatements = new HashMap<>();
    public Connection connection;
    public List<Statement> statementList = new ArrayList<>();
    public Map<String, Map<String, String>> schema = new HashMap<>();
    public String drivername;


    public JdbcSink(String url, String username, String password, String database, int batchSize, long interval) {
        super(url, username, password, database, batchSize, interval);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(this.drivername);
        createConnection();
    }

    @Override
    public void createConnection() throws Exception {


        this.connection = DriverManager.getConnection(url, username, password);
        connection.setAutoCommit(false);

        Statement statement = connection.createStatement();
        String sql = "create database if not exists " + this.database;
        statement.executeUpdate(sql);


    }

    public void preparedStatement(PreparedStatement preparedStatement, Operator sqlOperator, Map<String, String> columns) throws Exception {
        preparedStatement.setTimestamp(1, new Timestamp(sqlOperator.getOpts()));
        preparedStatement.setString(2, sqlOperator.getOpType());
        int i = 3;
        for (Map.Entry<String, String> entry : columns.entrySet()) {
            preparedStatement.setObject(i, sqlOperator.getColumnsValue().get(entry.getKey()));
            i++;
        }
        preparedStatement.addBatch();
    }

    @Override
    public void saveRecords() throws Exception {
        for (String table : preparedStatements.keySet()) {
            PreparedStatement preparedStatement = preparedStatements.get(table);
            try {
                preparedStatement.executeBatch();
            } catch (ClickHouseException e) {
                log.error("", e);
            }
            this.connection.commit();
            preparedStatement.clearBatch();
        }
    }


    @Override
    public void close() throws Exception {

        for (Statement statement : this.statementList) {
            if (null != statement) {
                statement.close();
            }
        }
        this.statementList.clear();

        for (PreparedStatement preparedStatement : this.preparedStatements.values()) {
            if (null != preparedStatement) {
                preparedStatement.close();
            }
        }
        this.preparedStatements.clear();




        connection.close();
    }

    @Override
    public void processData(Operator sqlOperator) throws Exception {


        PreparedStatement preparedStatement;
        Map<String, String> columns;
        List<String> addColumns = new ArrayList<>();

        if (!schema.containsKey(sqlOperator.getTablename())) {
            columns = getColumnsInfo(sqlOperator);
            schema.put(sqlOperator.getTablename(), columns);

            String createSql = getCreateSql(sqlOperator, columns);
            if (createSql != null) {
                Statement statement = connection.createStatement();
                statement.executeUpdate(createSql);
            } else {
                for (Map.Entry<String, String> entry : columns.entrySet()) {
                    String sql = getAddColumnsSql(sqlOperator.getTablename(), entry.getKey(), entry.getValue());
                    if (sql != null) {
                        Statement st = connection.createStatement();
                        st.executeUpdate(sql);
                    }
                }
            }
        }

        columns = schema.get(sqlOperator.getTablename());

        //这里判断有没有新增列
//        List<String> addColumns = new ArrayList<>();
        for (String key : sqlOperator.getColumnsType().keySet()) {
            if (!columns.containsKey(key)) {
                addColumns.add(key);
            }
        }

        if (addColumns.size() > 0) {
            columns = getColumnsInfo(sqlOperator);
            schema.put(sqlOperator.getTablename(), columns);

            preparedStatements.get(sqlOperator.getTablename()).executeBatch();
            connection.commit();
            preparedStatements.get(sqlOperator.getTablename()).clearBatch();
            preparedStatements.get(sqlOperator.getTablename()).close();
            preparedStatements.remove(sqlOperator.getTablename());

            for (String col : addColumns) {
                String sql = getAddColumnsSql(sqlOperator.getTablename(), col, columns.get(col));
                if (sql != null) {
                    Statement st = connection.createStatement();
                    st.executeUpdate(sql);
                }
            }
        }

        if (preparedStatements.containsKey(sqlOperator.getTablename())) {
            preparedStatement = preparedStatements.get(sqlOperator.getTablename());
        } else {
            String insertSql = getInsertSql(sqlOperator, columns);
            preparedStatement = connection.prepareStatement(insertSql);
            preparedStatements.put(sqlOperator.getTablename(), preparedStatement);
        }


        preparedStatement(preparedStatement, sqlOperator, columns);
    }

    abstract String getCreateSql(Operator row, Map<String, String> cols) throws Exception;

    public StringBuilder commonCreateSql(Operator row, Map<String, String> cols) {
        StringBuilder sql = new StringBuilder("create table if not exists " + this.database + ".");
        sql.append("`" + row.getTablename()  + "`(");

        String columns = "";
        int i = 0;
        for (Map.Entry<String, String> entry : cols.entrySet()) {
            if (i == cols.size() - 1) {
                columns += "`" + entry.getKey() + "` " + cols.get(entry.getKey());
            } else {
                columns += "`" + entry.getKey() + "` " + cols.get(entry.getKey()) + ",";
            }
            i++;
        }

        sql.append(columns);
        return sql;
    }

    public String commonPrSql(List<String> prs) {
        int j = 0;
        String ps = "";
        if (prs != null && prs.size() > 0) {
            ps = "(";
            for (String pri : prs) {
                if (j == prs.size() - 1) {
                    ps += "`" + pri + "`)";
                } else {
                    ps += "`" + pri + "`,";
                }
                j++;
            }
        }
        return ps;
    }

    public String getAddColumnsSql(String tableName, String column, String type) throws Exception {
        StringBuilder sql = new StringBuilder("alter table  " + this.database + ".");
        sql.append("`" + tableName + "`");
        sql.append(" add column IF NOT EXISTS ");
        sql.append("`" + column + "` ");
        sql.append(type);
        return sql.toString();
    }

    public String getInsertSql(Operator row, Map<String, String> cols) {
        StringBuilder sql = new StringBuilder();
        String columns = "(`dbz_op_time`,`dbz_op_type`,";
        String vals = "(?,?,";
        int i = 0;
        for (Map.Entry<String, String> entry : cols.entrySet()) {
            if (i == cols.size() - 1) {
                columns += "`" + entry.getKey() + "`)";
                vals += "?)";
            } else {
                columns += "`" + entry.getKey() + "`,";
                vals += "?,";
            }
            i++;
        }
        sql.append("insert into " + this.database + "." + "`" + row.getTablename() + "`");
        sql.append(columns);
        sql.append(" values ");
        sql.append(vals);
        return sql.toString();
    }

    Map<String, String> getColumnsInfo(Operator sqlOperator) throws Exception {
        Map<String, String> colums = sqlOperator.getColumnsType();
        for (String key : colums.keySet()) {
            if (sqlOperator.getPr().containsKey(key)) {
                colums.put(key, columntypetransfer(colums.get(key), true));
            } else {
                colums.put(key, columntypetransfer(colums.get(key), false));
            }
        }
        return colums;
    }

    abstract String columntypetransfer(String type, Boolean flag) throws Exception;


    @Override
    public void invoke(RowData value, Context context) throws Exception {
        if (value == null) {
            return;
        }

        Operator kingbaseOperator = deserialize(value);
        processData(kingbaseOperator);
        elements.add(value.getString(0).toString());

        if (elements.size() >= batchSize) {
            saveRecords();
            elements.clear();
        }
    }
}
