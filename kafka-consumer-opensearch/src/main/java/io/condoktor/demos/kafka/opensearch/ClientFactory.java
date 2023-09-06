package io.condoktor.demos.kafka.opensearch;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;

import java.net.URI;
import java.util.Optional;

public class ClientFactory {

    public static RestHighLevelClient createOpenSearchClient(URI connectionUri) {
        return Optional.ofNullable(connectionUri.getUserInfo()).map(userInfo -> {
            var auth                = userInfo.split(":");
            var credentialProviders = new BasicCredentialsProvider();

            credentialProviders.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            return new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost(connectionUri.getHost(), connectionUri.getPort(), "http")
                    ).setHttpClientConfigCallback(builder ->
                            builder.setDefaultCredentialsProvider(credentialProviders)
                                    .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())
                    )
            );
        }).orElse(
                new RestHighLevelClient(
                        RestClient.builder(
                                new HttpHost(connectionUri.getHost(), connectionUri.getPort(), "http")))
        );
    }
}
