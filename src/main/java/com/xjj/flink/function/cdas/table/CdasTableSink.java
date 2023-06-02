package com.xjj.flink.function.cdas.table;

import com.xjj.flink.function.cdas.enums.SinkTypeEnums;
import com.xjj.flink.function.cdas.table.elasticsearch.ElasticsearchConfiguration;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.types.RowKind;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * @author zhoujuncheng
 * @date 2022/5/12
 */
public class CdasTableSink implements DynamicTableSink {

    private final ResolvedSchema physicalSchema;
    private final String sinkType;
    private final String url;
    private final String database;
    private final String username;
    private final String password;
    private final String driver;
    private final String mergeKey;
    private final int batchSize;
    private final Long interval;
    private final Configuration configuration;


    public CdasTableSink(
            ResolvedSchema physicalSchema,
            String sinkType,
            String url,
            String database,
            String username,
            String password,
            int batchSize,
            Long inteval,
            String driver,
            String mergeKey,
            Configuration configuration) {

        this.physicalSchema = physicalSchema;
        this.sinkType = sinkType;
        this.url = url;
        this.database = database;
        this.username = username;
        this.password = password;
        this.driver = driver;
        this.mergeKey = mergeKey;
        this.batchSize = batchSize;
        this.interval = inteval;
        this.configuration = configuration;
    }


    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {

        AbstractCdasSinkFunction sink = null;

        if (SinkTypeEnums.CLICKHOUSE.getType().equals(sinkType)) {
            sink = new ClinkhouseSink(url, username, password, database, batchSize, interval,mergeKey);
        } else if (SinkTypeEnums.MYSQL.getType().equals(sinkType)) {
            sink = new MysqlSink(url, username, password, database, batchSize, interval);
        } else if (SinkTypeEnums.KINGBASE.getType().equals(sinkType)) {
            sink = new KingbaseSink(url, username, password, database, batchSize, interval,driver);
        } else if (SinkTypeEnums.MONGO.getType().equals(sinkType)) {
            sink = new MongoSink(url, username, password, database, batchSize, interval);
        } else if (SinkTypeEnums.ORACLE.getType().equals(sinkType)) {
            sink = new OracleSink(url, username, password, database, batchSize, interval);
        } else if (SinkTypeEnums.ELASTICSEARCH.getType().equals(sinkType)) {
            ElasticsearchConfiguration config =
                    new ElasticsearchConfiguration(configuration);
            List<String> hosts = Arrays.asList(url.split(","));

            final ElasticsearchSink.Builder builder =
                    new ElasticsearchSink.Builder(hosts, config);

            ElasticsearchSink sink1 = builder.build();
            return SinkFunctionProvider.of(sink1, 1);
        }

        return SinkFunctionProvider.of(sink, 1);
    }

    @Override
    public DynamicTableSink copy() {
        CdasTableSink sink =
                new CdasTableSink(
                        physicalSchema,
                        sinkType,
                        url,
                        database,
                        username,
                        password,
                        batchSize,
                        interval,
                        driver,
                        mergeKey,
                        configuration);

        return sink;
    }

    @Override
    public String asSummaryString() {
        return "Cdas-CDC";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CdasTableSink that = (CdasTableSink) o;
        return sinkType == that.sinkType
                && Objects.equals(physicalSchema, that.physicalSchema)
                && Objects.equals(url, that.url)
                && Objects.equals(database, that.database)
                && Objects.equals(username, that.username)
                && Objects.equals(password, that.password)
                && Objects.equals(batchSize, that.batchSize)
                && Objects.equals(interval, that.interval)
                && Objects.equals(driver, that.driver)
                && Objects.equals(mergeKey, that.mergeKey)
                && Objects.equals(configuration, that.configuration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                physicalSchema,
                sinkType,
                url,
                database,
                username,
                password,
                driver,
                mergeKey,
                batchSize,
                interval,
                configuration);
    }
}
