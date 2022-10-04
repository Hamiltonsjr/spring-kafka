package br.com.kafka.ecommerce.kafka.producer;

import br.com.kafka.ecommerce.kafka.Topics;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Service;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
@Service
public class ProducerMessage {

    public void sendMessage() throws ExecutionException, InterruptedException {
        try (var producer = new KafkaProducer<String, String>(properties())) {
            var value = "123,456,789";
            var record = new ProducerRecord<>(Topics.ECOMMERCE_ORDER, value, value);
            Callback callback = (data, ex) -> {
                if (ex != null) {
                    ex.printStackTrace();
                }
                log.info("Success data={} ::partition={} ::offset={}", data.topic(), data.partition(), data.offset());
            };
            var email = "Welcome";
            var emailRecord = new ProducerRecord<>(Topics.EMAIL, email, email);
            producer.send(record, callback).get();
            producer.send(emailRecord, callback).get();
        }
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
