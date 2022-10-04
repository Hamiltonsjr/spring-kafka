package br.com.kafka.ecommerce.kafka.producer;

import br.com.kafka.ecommerce.kafka.KafkaDispatcher;
import br.com.kafka.ecommerce.kafka.Topics;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Service;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@Slf4j
@Service
public class ProducerMessage {

    public void sendMessage() throws ExecutionException, InterruptedException {
        var dispatcher = new KafkaDispatcher();

        final String key = UUID.randomUUID().toString();
        var value = "123,456,789";

        var email = "Welcome";
        dispatcher.send(Topics.ECOMMERCE_ORDER, key, value);
        dispatcher.send(Topics.EMAIL,key,email);
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // serializados das chaves
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

}
