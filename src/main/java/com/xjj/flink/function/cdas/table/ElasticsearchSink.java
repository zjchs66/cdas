package com.xjj.flink.function.cdas.table;
import com.xjj.flink.function.cdas.table.elasticsearch.*;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author zhoujuncheng
 * @date 2022/6/17
 */
public class ElasticsearchSink extends ElasticsearchSinkBase<RestHighLevelClient> {


    public ElasticsearchSink(
            List<HttpHost> httpHosts,
            RestClientFactory restClientFactory,
            Map<String, String> userConfig,
            ActionRequestFailureHandler failureHandler) {

        super(
                new Elasticsearch7ApiCallBridge(httpHosts, restClientFactory),
                userConfig,
                failureHandler);


    }


    /**
     * A builder for creating an {@link ElasticsearchSink}.
     */
    @PublicEvolving
    public static class Builder {

        private final List<HttpHost> httpHosts;
        private List<String> url;
        private Map<String, String> bulkRequestsConfig = new HashMap<>();
        private ActionRequestFailureHandler failureHandler = new NoOpFailureHandler();
        private ElasticsearchConfiguration elasticsearchConfiguration;
        private RestClientFactory restClientFactory = restClientBuilder -> {
        };

        /**
         * Creates a new {@code ElasticsearchSink} that connects to the cluster using a {@link
         * RestHighLevelClient}.
         */
        public Builder(
                List<String> url, ElasticsearchConfiguration elasticsearchConfiguration) {
            this.url = Preconditions.checkNotNull(url);
            this.elasticsearchConfiguration = Preconditions.checkNotNull(elasticsearchConfiguration);
            this.httpHosts = getHosts();
        }

        public List<HttpHost> getHosts() {
            return url.stream()
                    .map(this::validateAndParseHostsString)
                    .collect(Collectors.toList());
        }

        private HttpHost validateAndParseHostsString(String host) {
            try {
                HttpHost httpHost = HttpHost.create(host);
                if (httpHost.getPort() < 0) {
                    throw new ValidationException(
                            String.format(
                                    "Could not parse host '%s' in option '%s'. It should follow the format 'http://host_name:port'. Missing port.",
                                    host, "url"));
                }

                if (httpHost.getSchemeName() == null) {
                    throw new ValidationException(
                            String.format(
                                    "Could not parse host '%s' in option '%s'. It should follow the format 'http://host_name:port'. Missing scheme.",
                                    host, "url"));
                }
                return httpHost;
            } catch (Exception e) {
                throw new ValidationException(
                        String.format(
                                "Could not parse host '%s' in option '%s'. It should follow the format 'http://host_name:port'.",
                                host, "url"),
                        e);
            }
        }

        /**
         * Sets the maximum number of actions to buffer for each bulk request. You can pass -1 to
         * disable it.
         *
         * @param numMaxActions the maximum number of actions to buffer per bulk request.
         */
        public void setBulkFlushMaxActions(int numMaxActions) {
            Preconditions.checkArgument(
                    numMaxActions == -1 || numMaxActions > 0,
                    "Max number of buffered actions must be larger than 0.");

            this.bulkRequestsConfig.put(
                    CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, String.valueOf(numMaxActions));
        }

        /**
         * Sets the maximum size of buffered actions, in mb, per bulk request. You can pass -1 to
         * disable it.
         *
         * @param maxSizeMb the maximum size of buffered actions, in mb.
         */
        public void setBulkFlushMaxSizeMb(int maxSizeMb) {
            Preconditions.checkArgument(
                    maxSizeMb == -1 || maxSizeMb > 0,
                    "Max size of buffered actions must be larger than 0.");

            this.bulkRequestsConfig.put(
                    CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB, String.valueOf(maxSizeMb));
        }

        /**
         * Sets the bulk flush interval, in milliseconds. You can pass -1 to disable it.
         *
         * @param intervalMillis the bulk flush interval, in milliseconds.
         */
        public void setBulkFlushInterval(long intervalMillis) {
            Preconditions.checkArgument(
                    intervalMillis == -1 || intervalMillis >= 0,
                    "Interval (in milliseconds) between each flush must be larger than or equal to 0.");

            this.bulkRequestsConfig.put(
                    CONFIG_KEY_BULK_FLUSH_INTERVAL_MS, String.valueOf(intervalMillis));
        }

        /**
         * Sets whether or not to enable bulk flush backoff behaviour.
         *
         * @param enabled whether or not to enable backoffs.
         */
        public void setBulkFlushBackoff(boolean enabled) {
            this.bulkRequestsConfig.put(
                    CONFIG_KEY_BULK_FLUSH_BACKOFF_ENABLE, String.valueOf(enabled));
        }

        /**
         * Sets the type of back of to use when flushing bulk requests.
         *
         * @param flushBackoffType the backoff type to use.
         */
        public void setBulkFlushBackoffType(FlushBackoffType flushBackoffType) {
            this.bulkRequestsConfig.put(
                    CONFIG_KEY_BULK_FLUSH_BACKOFF_TYPE,
                    Preconditions.checkNotNull(flushBackoffType).toString());
        }

        /**
         * Sets the maximum number of retries for a backoff attempt when flushing bulk requests.
         *
         * @param maxRetries the maximum number of retries for a backoff attempt when flushing bulk
         *                   requests
         */
        public void setBulkFlushBackoffRetries(int maxRetries) {
            Preconditions.checkArgument(
                    maxRetries > 0, "Max number of backoff attempts must be larger than 0.");

            this.bulkRequestsConfig.put(
                    CONFIG_KEY_BULK_FLUSH_BACKOFF_RETRIES, String.valueOf(maxRetries));
        }

        /**
         * Sets the amount of delay between each backoff attempt when flushing bulk requests, in
         * milliseconds.
         *
         * @param delayMillis the amount of delay between each backoff attempt when flushing bulk
         *                    requests, in milliseconds.
         */
        public void setBulkFlushBackoffDelay(long delayMillis) {
            Preconditions.checkArgument(
                    delayMillis >= 0,
                    "Delay (in milliseconds) between each backoff attempt must be larger than or equal to 0.");
            this.bulkRequestsConfig.put(
                    CONFIG_KEY_BULK_FLUSH_BACKOFF_DELAY, String.valueOf(delayMillis));
        }

        /**
         * Sets a failure handler for action requests.
         *
         * @param failureHandler This is used to handle failed {@link ActionRequest}.
         */
        public void setFailureHandler(ActionRequestFailureHandler failureHandler) {
            this.failureHandler = Preconditions.checkNotNull(failureHandler);
        }

        /**
         * Sets a REST client factory for custom client configuration.
         *
         * @param restClientFactory the factory that configures the rest client.
         */
        public void setRestClientFactory(RestClientFactory restClientFactory) {
            this.restClientFactory = Preconditions.checkNotNull(restClientFactory);
        }

        public ElasticsearchSink build() {
            this.setFailureHandler(elasticsearchConfiguration.getFailureHandler());
            this.setBulkFlushMaxActions(elasticsearchConfiguration.getBulkFlushMaxActions());
            this.setBulkFlushMaxSizeMb((int) (elasticsearchConfiguration.getBulkFlushMaxByteSize() >> 20));
            this.setBulkFlushInterval(elasticsearchConfiguration.getBulkFlushInterval());
            this.setBulkFlushBackoff(elasticsearchConfiguration.isBulkFlushBackoffEnabled());
            elasticsearchConfiguration.getBulkFlushBackoffType().ifPresent(this::setBulkFlushBackoffType);
            elasticsearchConfiguration.getBulkFlushBackoffRetries().ifPresent(this::setBulkFlushBackoffRetries);
            elasticsearchConfiguration.getBulkFlushBackoffDelay().ifPresent(this::setBulkFlushBackoffDelay);

            if (elasticsearchConfiguration.getUsername().isPresent()
                    && elasticsearchConfiguration.getPassword().isPresent()
                    && !StringUtils.isNullOrWhitespaceOnly(elasticsearchConfiguration.getUsername().get())
                    && !StringUtils.isNullOrWhitespaceOnly(elasticsearchConfiguration.getPassword().get())) {
                this.setRestClientFactory(
                        new ElasticsearchSink.AuthRestClientFactory(
                                elasticsearchConfiguration.getPathPrefix().orElse(null),
                                elasticsearchConfiguration.getUsername().get(),
                                elasticsearchConfiguration.getPassword().get()));
            } else {
                this.setRestClientFactory(
                        new ElasticsearchSink.DefaultRestClientFactory(elasticsearchConfiguration.getPathPrefix().orElse(null)));
            }


            ElasticsearchSink sink = new ElasticsearchSink(
                    httpHosts,
                    restClientFactory,
                    bulkRequestsConfig,
                    failureHandler);

            if (elasticsearchConfiguration.isDisableFlushOnCheckpoint()) {
                sink.disableFlushOnCheckpoint();
            }

            return sink;
        }

    }

    /**
     * Serializable {@link RestClientFactory} used by the sink.
     */
    @VisibleForTesting
    static class DefaultRestClientFactory implements RestClientFactory {

        private final String pathPrefix;

        public DefaultRestClientFactory(@Nullable String pathPrefix) {
            this.pathPrefix = pathPrefix;
        }

        @Override
        public void configureRestClientBuilder(RestClientBuilder restClientBuilder) {
            if (pathPrefix != null) {
                restClientBuilder.setPathPrefix(pathPrefix);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DefaultRestClientFactory that = (DefaultRestClientFactory) o;
            return Objects.equals(pathPrefix, that.pathPrefix);
        }

        @Override
        public int hashCode() {
            return Objects.hash(pathPrefix);
        }
    }

    /**
     * Serializable {@link RestClientFactory} used by the sink which enable authentication.
     */
    @VisibleForTesting
    static class AuthRestClientFactory implements RestClientFactory {

        private final String pathPrefix;
        private final String username;
        private final String password;
        private transient CredentialsProvider credentialsProvider;

        public AuthRestClientFactory(
                @Nullable String pathPrefix, String username, String password) {
            this.pathPrefix = pathPrefix;
            this.password = password;
            this.username = username;
        }

        @Override
        public void configureRestClientBuilder(RestClientBuilder restClientBuilder) {
            if (pathPrefix != null) {
                restClientBuilder.setPathPrefix(pathPrefix);
            }
            if (credentialsProvider == null) {
                credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(
                        AuthScope.ANY, new UsernamePasswordCredentials(username, password));
            }
            restClientBuilder.setHttpClientConfigCallback(
                    httpAsyncClientBuilder ->
                            httpAsyncClientBuilder.setDefaultCredentialsProvider(
                                    credentialsProvider));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AuthRestClientFactory that = (AuthRestClientFactory) o;
            return Objects.equals(pathPrefix, that.pathPrefix)
                    && Objects.equals(username, that.username)
                    && Objects.equals(password, that.password);
        }

        @Override
        public int hashCode() {
            return Objects.hash(pathPrefix, password, username);
        }
    }
}
