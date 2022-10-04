package br.com.kafka.ecommerce.kafka.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.regex.Pattern;

@Slf4j
@Service
public class LogService {

    public void logConsumer() {
        var consumer = new LogService();
        var service = new KafkaService(LogService.class.getSimpleName(),
                Pattern.compile("fct.*"), consumer::parse, String.class,
                Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getSimpleName()));
        service.run();
    }

    private void parse(final ConsumerRecord<String, String> record) {
        log.info("LOG={}", record.topic());
        log.info("Key={}", record.key());
        log.info("Value={}", record.value());
    }

}
