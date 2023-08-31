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
import java.util.HashMap;
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
        var producer = new KafkaProducer<String, String>(Map.ofEntries(
                new AbstractMap.SimpleEntry<>(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092"),
                new AbstractMap.SimpleEntry<>(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()),
                new AbstractMap.SimpleEntry<>(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
        ));
        return new WikimediaChangeHandler(producer, topic);
    }

    private static BackgroundEventSource getBackgroundEventSource(WikimediaChangeHandler handler, String url) {
        var eventSourceBuilder = new EventSource.Builder(URI.create(url));
        return new BackgroundEventSource.Builder(handler, eventSourceBuilder).build();
    }
}
