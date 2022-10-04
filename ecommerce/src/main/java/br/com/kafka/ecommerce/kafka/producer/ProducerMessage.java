package br.com.kafka.ecommerce.kafka.producer;

import br.com.kafka.ecommerce.kafka.KafkaDispatcher;
import br.com.kafka.ecommerce.kafka.Topics;
import br.com.kafka.ecommerce.kafka.domain.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Service;
import org.springframework.util.NumberUtils;

import java.math.BigDecimal;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@Slf4j
@Service
public class ProducerMessage {

    public void sendMessage() throws ExecutionException, InterruptedException {
        var orderDispatcher = new KafkaDispatcher<Order>();
        var emailDispatcher = new KafkaDispatcher<String>();

        var email = "Welcome";
        orderDispatcher.send(Topics.ECOMMERCE_ORDER, this.createOrder().getUserId(), this.createOrder());
        emailDispatcher.send(Topics.EMAIL, UUID.randomUUID().toString(), email);
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // serializados das chaves
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    private Order createOrder() {
        return Order.builder()
                .userId(UUID.randomUUID().toString())
                .orderId(UUID.randomUUID().toString())
                .amount(BigDecimal.valueOf(100))
                .build();
    }

}
