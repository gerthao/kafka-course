package io.condoktor.demos.kafka.opensearch;

import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Objects;
import java.util.Optional;

public class RecordDataExtractor {
    public static Optional<String> extractId(ConsumerRecord<String, String> record) {
        return Optional.ofNullable(record.value())
                .map(v -> JsonParser.parseString(v)
                        .getAsJsonObject()
                        .get("meta")
                        .getAsJsonObject()
                        .get("id")
                        .getAsString()
                );
    }
}
