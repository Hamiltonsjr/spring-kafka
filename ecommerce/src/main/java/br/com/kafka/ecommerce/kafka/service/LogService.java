package br.com.kafka.ecommerce.kafka.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

import java.util.regex.Pattern;

@Slf4j
@Service
public class LogService {

    public void logConsumer() {
        var consumer = new LogService();
        var service = new KafkaService(LogService.class.getSimpleName(), Pattern.compile("fct.*"), consumer::parse);
        service.run();
    }

    private void parse(final ConsumerRecord<String, String> record) {
        log.info("LOG={}", record.topic());
        log.info("Key={}", record.key());
        log.info("Value={}", record.value());
    }

}
