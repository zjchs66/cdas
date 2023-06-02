
package com.xjj.flink.function.cdas.table.elasticsearch;

import com.xjj.flink.function.cdas.table.ElasticsearchSink;
import org.apache.flink.configuration.ReadableConfig;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

import static com.xjj.flink.function.cdas.table.elasticsearch.ElasticsearchConnectorOptions.*;


/**
 * @author zhoujuncheng
 * @date 2022/6/17
 */
public class ElasticsearchConfiguration {
    protected final ReadableConfig config;

    public ElasticsearchConfiguration(ReadableConfig config) {
        this.config = config;
    }


    public ActionRequestFailureHandler getFailureHandler() {
        final ActionRequestFailureHandler failureHandler;
        String value = config.get(FAILURE_HANDLER_OPTION);
        switch (value.toUpperCase()) {
            case "FAIL":
                failureHandler = new NoOpFailureHandler();
                break;
            case "IGNORE":
                failureHandler = new IgnoringFailureHandler();
                break;
            case "RETRY-REJECTED":
                failureHandler = new RetryRejectedExecutionFailureHandler();
                break;
            default:
                failureHandler = new NoOpFailureHandler();
                break;
        }
        return failureHandler;
    }

    public String getDocumentType() {
        return config.get(ElasticsearchConnectorOptions.DOCUMENT_TYPE_OPTION);
    }

    public int getBulkFlushMaxActions() {
        int maxActions = config.get(ElasticsearchConnectorOptions.BULK_FLUSH_MAX_ACTIONS_OPTION);
        // convert 0 to -1, because Elasticsearch client use -1 to disable this configuration.
        return maxActions == 0 ? -1 : maxActions;
    }

    public long getBulkFlushMaxByteSize() {
        long maxSize =
                config.get(ElasticsearchConnectorOptions.BULK_FLASH_MAX_SIZE_OPTION).getBytes();
        // convert 0 to -1, because Elasticsearch client use -1 to disable this configuration.
        return maxSize == 0 ? -1 : maxSize;
    }

    public long getBulkFlushInterval() {
        long interval = config.get(BULK_FLUSH_INTERVAL_OPTION).toMillis();
        // convert 0 to -1, because Elasticsearch client use -1 to disable this configuration.
        return interval == 0 ? -1 : interval;
    }

    public Optional<String> getUsername() {
        return config.getOptional(USERNAME_OPTION);
    }

    public Optional<String> getPassword() {
        return config.getOptional(PASSWORD_OPTION);
    }

    public boolean isBulkFlushBackoffEnabled() {
        return config.get(BULK_FLUSH_BACKOFF_TYPE_OPTION)
                != ElasticsearchConnectorOptions.BackOffType.DISABLED;
    }

    public Optional<ElasticsearchSink.FlushBackoffType> getBulkFlushBackoffType() {
        switch (config.get(BULK_FLUSH_BACKOFF_TYPE_OPTION)) {
            case CONSTANT:
                return Optional.of(ElasticsearchSink.FlushBackoffType.CONSTANT);
            case EXPONENTIAL:
                return Optional.of(ElasticsearchSink.FlushBackoffType.EXPONENTIAL);
            default:
                return Optional.empty();
        }
    }

    public Optional<Integer> getBulkFlushBackoffRetries() {
        return config.getOptional(BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION);
    }

    public Optional<Long> getBulkFlushBackoffDelay() {
        return config.getOptional(BULK_FLUSH_BACKOFF_DELAY_OPTION).map(Duration::toMillis);
    }

    public boolean isDisableFlushOnCheckpoint() {
        return !config.get(ElasticsearchConnectorOptions.FLUSH_ON_CHECKPOINT_OPTION);
    }

    public String getIndex() {
        return config.get(ElasticsearchConnectorOptions.INDEX_OPTION);
    }

    public String getKeyDelimiter() {
        return config.get(ElasticsearchConnectorOptions.KEY_DELIMITER_OPTION);
    }

    public Optional<String> getPathPrefix() {
        return config.getOptional(ElasticsearchConnectorOptions.CONNECTION_PATH_PREFIX);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ElasticsearchConfiguration that = (ElasticsearchConfiguration) o;
        return Objects.equals(config, that.config);
    }

    @Override
    public int hashCode() {
        return Objects.hash(config);
    }
}
