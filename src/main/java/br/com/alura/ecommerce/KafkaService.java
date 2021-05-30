package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public class KafkaService {
    private String topic;
    private ConsumerFunction parse;
    private String groupId;

    public KafkaService(String groupId, String topic, ConsumerFunction parse) {
        this.topic = topic;
        this.parse = parse;
        this.groupId = groupId;
    }

    public void run() {
        var consumer = new KafkaConsumer<String, String>(properties(this.groupId));
        consumer.subscribe(Collections.singletonList(this.topic));
        while(true) {
            var records = consumer.poll(Duration.ofMillis(100));
            if(records.isEmpty()) {
                continue;
            }
            for(var record: records) {
                this.parse.consume(record);
            }
        }
    }

    private static Properties properties(String groupId) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        return properties;
    }
}
