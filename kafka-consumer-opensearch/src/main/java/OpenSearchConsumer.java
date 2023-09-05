import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.opensearch.client.OpenSearchClient;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

public class OpenSearchConsumer {
    private static final Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

    public static void main(String[] args) {
        try (RestHighLevelClient client = createOpenSearchClient(URI.create("http://localhost:9200"))) {
            // create an index on OpenSearch client
            createIndex(client, "wikimedia");

        } catch (Throwable e) {
            log.error("Unexpected error occurred in OpenSearchConsumer.", e);
        }

    }

    private static void createIndex(RestHighLevelClient client, String index) throws IOException {
        if (!client.indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT)) {
            client.indices().create(new CreateIndexRequest(index), RequestOptions.DEFAULT);
            log.info("Index: " + index + " has been created.");
        } else {
            log.info("Index: " + index + " already exists.");
        }
    }

    private static RestHighLevelClient createOpenSearchClient(URI connectionUri) {
        RestHighLevelClient client;

        var userInfo = connectionUri.getUserInfo();

        if (userInfo == null) {
            client = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost(connectionUri.getHost(), connectionUri.getPort(), "http")
                    )
            );
        } else {
            var auth                = userInfo.split(":");
            var credentialProviders = new BasicCredentialsProvider();

            credentialProviders.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            client = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost(connectionUri.getHost(), connectionUri.getPort(), connectionUri.getScheme())
                    ).setHttpClientConfigCallback(builder ->
                            builder.setDefaultCredentialsProvider(credentialProviders)
                                    .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())
                    )
            );
        }

        return client;
    }
}
