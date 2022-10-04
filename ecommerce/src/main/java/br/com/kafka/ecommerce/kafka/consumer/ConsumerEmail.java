package br.com.kafka.ecommerce.kafka.consumer;

import br.com.kafka.ecommerce.kafka.Topics;
import br.com.kafka.ecommerce.kafka.service.KafkaService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Properties;

@Slf4j
@Service
public class ConsumerEmail {

    public void consumerEmail() {
        var consumerEmail = new ConsumerEmail();
        var service = new KafkaService(ConsumerEmail.class.getSimpleName(),
                Topics.EMAIL, consumerEmail::parse, String.class, new HashMap<>());
        service.run();
    }

    private void parse(final ConsumerRecord<String, String> record) {
        log.info("Processeing");
        log.info("Key={}", record.key());
        log.info("Value={}", record.value());
        log.info("Partition={}", record.partition());
    }

    private static Properties propertiesEmail() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, ConsumerEmail.class.getSimpleName());
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, ConsumerEmail.class.getSimpleName());
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        return properties;
    }
}
