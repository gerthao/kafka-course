package io.condoktor.demos.kafka.opensearch;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;

public class OpenSearchConsumer {
    private static final Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

    public static void main(String[] args) {
        final Thread mainThread = Thread.currentThread();

        try (RestHighLevelClient client = ClientFactory.createOpenSearchClient(URI.create("http://localhost:9200"));
             KafkaConsumer<String, String> consumer = ConsumerFactory.create("127.0.0.1:9092", "consumer-opensearch-demo")) {
            addShutdownHook(consumer, mainThread);

            // create an index on OpenSearch client
            createInitialIndex(client, "wikimedia");
            consumer.subscribe(Collections.singleton("wikimedia.recentchange"));

            var runner = new OpenSearchConsumerRunner(client, consumer);

            while (true) {
                runner.execute(Duration.ofMillis(3000));
            }
        } catch (WakeupException e) {
            log.info("Consumer is starting to shut down...");
        } catch (Exception e) {
            log.error("Unexpected error occurred in consumer.  Stopping application...", e);
        } finally {
            log.info("Consumer is now gracefully shut down");
        }

    }

    private static void addShutdownHook(KafkaConsumer<String, String> consumer, Thread mainThread) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected a shutdown, waking up the consumer...");
            consumer.wakeup();

            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
    }

    private static void createInitialIndex(RestHighLevelClient client, String index) throws IOException {
        if (!client.indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT)) {
            client.indices().create(new CreateIndexRequest(index), RequestOptions.DEFAULT);
            log.info("Index: " + index + " has been created.");
        } else {
            log.info("Index: " + index + " already exists.");
        }
    }
}
