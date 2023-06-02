package com.xjj.flink.function.cdas.table;


import com.xjj.flink.function.cdas.entity.Operator;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhoujuncheng
 * @date 2022/4/28
 */
@Slf4j
abstract public class AbstractCdasSinkFunction<T> extends RichSinkFunction<RowData>
        implements CheckpointedFunction, CommonOperation {

    public String url;
    public String username;
    public String password;
    public String database;
    public List<T> elements = new ArrayList<>();
    public int batchSize;
    public long interval;
    public ListState<T> listState;

    public AbstractCdasSinkFunction(String url, String username, String password, String database, int batchSize, long interval) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.database = database;
        this.batchSize = batchSize;
        this.interval = interval;
    }


    abstract void processData(Operator record) throws Exception;

    abstract void saveRecords() throws Exception;

    abstract void createConnection() throws Exception;


    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
       saveRecords();
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

    }
}
