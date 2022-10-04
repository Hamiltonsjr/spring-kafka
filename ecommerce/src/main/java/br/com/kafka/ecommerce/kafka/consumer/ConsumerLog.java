package br.com.kafka.ecommerce.kafka.consumer;

import br.com.kafka.ecommerce.kafka.Topics;
import br.com.kafka.ecommerce.kafka.service.KafkaService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Slf4j
@Service
public class ConsumerLog {

    public void logConsumer() {
//        var consumer = new KafkaConsumer<String, String>(properties());
//        // consumindo varios topicos
//        consumer.subscribe(Pattern.compile("fct.*"));
//        while (true) {
//            var records = consumer.poll(Duration.ofMillis(100));
//            if (!records.isEmpty()) {
//                log.info("message is empty");
//                for (var record : records) {
//                    log.info("LOG={}", record.topic());
//                    log.info("Key={}", record.key());
//                    log.info("Value={}", record.value());
//                }
//            }
//        }
        var consumer = new ConsumerLog();
        var service = new KafkaService(ConsumerLog.class.getSimpleName(), Topics.ECOMMERCE_ORDER, consumer::parse);
        service.run();
    }

    private void parse(final ConsumerRecord<String, String> record) {
        log.info("LOG={}", record.topic());
        log.info("Key={}", record.key());
        log.info("Value={}", record.value());

    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, ConsumerLog.class.getSimpleName());
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        return properties;
    }

}
