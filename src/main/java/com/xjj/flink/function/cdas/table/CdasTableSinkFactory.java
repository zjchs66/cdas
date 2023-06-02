package com.xjj.flink.function.cdas.table;

import com.xjj.flink.function.cdas.util.CommonConstant;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.xjj.flink.function.cdas.table.elasticsearch.ElasticsearchConnectorOptions.*;


/**
 * @author zhoujuncheng
 * @date 2022/5/13
 */
public class CdasTableSinkFactory implements DynamicTableSinkFactory {

    private static final String IDENTIFIER = "cdas-cdc";

    private static final Set<ConfigOption<?>> optionalOptions =
            Stream.of(
                    KEY_DELIMITER_OPTION,
                    FAILURE_HANDLER_OPTION,
                    FLUSH_ON_CHECKPOINT_OPTION,
                    BULK_FLASH_MAX_SIZE_OPTION,
                    BULK_FLUSH_MAX_ACTIONS_OPTION,
                    BULK_FLUSH_INTERVAL_OPTION,
                    BULK_FLUSH_BACKOFF_TYPE_OPTION,
                    BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION,
                    BULK_FLUSH_BACKOFF_DELAY_OPTION,
                    CONNECTION_MAX_RETRY_TIMEOUT_OPTION,
                    CONNECTION_PATH_PREFIX,
                    FORMAT_OPTION)
                    .collect(Collectors.toSet());

    private static final ConfigOption<String> SINK_TYPE =
            ConfigOptions.key("sink-type")
                    .stringType()
                    .noDefaultValue();

    private static final ConfigOption<String> URL =
            ConfigOptions.key("url")
                    .stringType()
                    .noDefaultValue();

    private static final ConfigOption<String> DRIVER =
            ConfigOptions.key("driver")
                    .stringType()
                    .noDefaultValue();

    private static final ConfigOption<String> MERGEKEY =
            ConfigOptions.key("mergeKey")
                    .stringType()
                    .noDefaultValue();

    private static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue();

    private static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue();

    private static final ConfigOption<String> DATABASE_NAME =
            ConfigOptions.key("database-name")
                    .stringType()
                    .noDefaultValue();


    private static final ConfigOption<Integer> BATCH_SIZE =
            ConfigOptions.key("batch-size")
                    .intType()
                    .defaultValue(5000);

    private static final ConfigOption<Long> INTERVAL =
            ConfigOptions.key("interval")
                    .longType()
                    .defaultValue(5000L);

    private static final ConfigOption<Boolean> APPEND_OP_KIND =
            ConfigOptions.key("append_op_kind")
                    .booleanType()
                    .defaultValue(false);

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        final ReadableConfig config = helper.getOptions();
        String sinkType = config.get(SINK_TYPE);
        String url = config.get(URL);
        String username = config.get(USERNAME);
        String password = config.get(PASSWORD);
        String databaseName = config.get(DATABASE_NAME);
        String driver = config.get(DRIVER);
        String mergeKey = config.get(MERGEKEY);
        Long interval = config.get(INTERVAL);
        CommonConstant.appendOpKind = config.get(APPEND_OP_KIND);

        int batchSize = config.get(BATCH_SIZE);
        ResolvedSchema physicalSchema = context.getCatalogTable().getResolvedSchema();

        Configuration configuration = new Configuration();
        context.getCatalogTable().getOptions().forEach(configuration::setString);


        return new CdasTableSink(
                physicalSchema,
                sinkType,
                url,
                databaseName,
                username,
                password,
                batchSize,
                interval,
                driver,
                mergeKey,
                configuration);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SINK_TYPE);
        options.add(URL);
        options.add(USERNAME);
        options.add(DATABASE_NAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(BATCH_SIZE);
        options.add(INTERVAL);
        options.add(PASSWORD);
        options.add(DRIVER);
        options.add(MERGEKEY);
        options.add(APPEND_OP_KIND);
        options.addAll(optionalOptions);
        return options;
    }
}
