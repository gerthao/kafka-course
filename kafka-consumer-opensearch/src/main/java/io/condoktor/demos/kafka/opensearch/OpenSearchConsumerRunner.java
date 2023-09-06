package io.condoktor.demos.kafka.opensearch;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;

public class OpenSearchConsumerRunner {
    private final        RestHighLevelClient           client;
    private final        KafkaConsumer<String, String> consumer;
    private static final Logger                        log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

    public OpenSearchConsumerRunner(RestHighLevelClient client, KafkaConsumer<String, String> consumer) {
        this.client   = client;
        this.consumer = consumer;
    }

    public void execute(Duration pollingTime) {
        var records     = consumer.poll(pollingTime);
        var count       = records.count();
        var bulkRequest = new BulkRequest();
        log.info("Received " + count + " record(s).");

        records.forEach(r -> insertIntoBulkRequest(r, bulkRequest));

        if (bulkRequest.numberOfActions() > 0) {
            try {
                var bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                log.info("Inserted " + bulkResponse.getItems().length + " record(s).");
                Thread.sleep(Duration.ofMillis(1000));
            } catch (IOException e) {
                log.error("Error while making a bulk request to client.", e);
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void close() throws IOException {
        this.client.close();
        this.consumer.close();
    }

    private void insertIntoBulkRequest(ConsumerRecord<String, String> record, BulkRequest bulkRequest) {
        RecordDataExtractor.extractId(record).ifPresentOrElse(id -> {
                    var request = new IndexRequest("wikimedia")
                            .source(record.value(), XContentType.JSON)
                            .id(id);

                    bulkRequest.add(request);
                },
                () -> log.warn("Could not get id for request.  Skipping to next request.")
        );
    }
}
