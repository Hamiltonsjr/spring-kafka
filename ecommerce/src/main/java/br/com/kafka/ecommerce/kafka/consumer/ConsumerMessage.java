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
public class ConsumerMessage {

    public void consumerMessage() {
        var consumer = new KafkaConsumer<String, String>(properties());
        // escutando o topico do envio da mensagem
        consumer.subscribe(Collections.singletonList(Topics.ECOMMERCE_ORDER));
        while (true) {
            // checar se contem mensagem por um tempo estimado
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

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // deserializadores das chaves
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // criação de grupos para ouvir os topicos
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, ConsumerMessage.class.getSimpleName());
        // rebalanceamento de consumo da mensagem fazendo o poll de 1 em 1
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        return properties;
    }

}
