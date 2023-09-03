package io.conduktor.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {
    private static final Logger logger = LoggerFactory.getLogger(WikimediaChangesProducer.class);

    public static void main(String[] args) {
        try {
            var topic                 = "wikimedia.recentchange";
            var url                   = "https://stream.wikimedia.org/v2/stream/recentchange";
            var handler               = getWikimediaChangeHandler(topic);
            var backgroundEventSource = getBackgroundEventSource(handler, url);

            // start the producer in another thread
            backgroundEventSource.start();

            // produce for 10 minutes and block the program until then
            TimeUnit.MINUTES.sleep(10);
        } catch (Exception e) {
            logger.error("Unexpected error occurred in application.  Shutting down...", e);
        }
    }

    @NotNull
    private static WikimediaChangeHandler getWikimediaChangeHandler(String topic) {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // set safe producer configs (only for Kafka <= 2.8, otherwise not needed to be set manually)
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString((Integer.MAX_VALUE)));
        // set high throughput producer configs
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        return new WikimediaChangeHandler(new KafkaProducer<>(properties), topic);
    }

    @NotNull
    private static BackgroundEventSource getBackgroundEventSource(WikimediaChangeHandler handler, String url) {
        var eventSourceBuilder = new EventSource.Builder(URI.create(url));
        return new BackgroundEventSource.Builder(handler, eventSourceBuilder).build();
    }
}
