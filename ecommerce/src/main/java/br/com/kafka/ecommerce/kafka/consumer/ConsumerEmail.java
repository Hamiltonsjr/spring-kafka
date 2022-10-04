package br.com.kafka.ecommerce.kafka.consumer;

import br.com.kafka.ecommerce.kafka.Topics;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
@Service
public class ConsumerEmail {

    public void consumerEmail() {
        var consumer = new KafkaConsumer<String, String>(propertiesEmail());
        consumer.subscribe(Collections.singletonList(Topics.EMAIL));
        while (true) {
            var records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()) {
                log.info("message is empty");
                for (var record : records) {
                    log.info("Processeing");
                    log.info("Key={}", record.key());
                    log.info("Value={}", record.value());
                    log.info("Partition={}", record.partition());
                }
            }
        }
    }

    private static Properties propertiesEmail() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, ConsumerEmail.class.getSimpleName());
        return properties;
    }
}
