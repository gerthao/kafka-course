package io.conduktor.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements BackgroundEventHandler {
    private Logger logger = LoggerFactory.getLogger(WikimediaChangeHandler.class);
    private KafkaProducer<String, String> producer;
    private String topic;

    public WikimediaChangeHandler(KafkaProducer<String, String> producer, String topic) {
        this.topic    = topic;
        this.producer = producer;
    }

    @Override
    public void onOpen() {

    }

    @Override
    public void onClosed() {
        producer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) {
        var data = messageEvent.getData();
        logger.info(data);
        producer.send(new ProducerRecord<>(topic, data));
    }

    @Override
    public void onComment(String comment) {

    }

    @Override
    public void onError(Throwable t) {
        logger.error("Error in stream reading...", t);
    }
}
