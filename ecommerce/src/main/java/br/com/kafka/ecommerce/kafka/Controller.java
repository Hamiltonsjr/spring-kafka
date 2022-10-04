package br.com.kafka.ecommerce.kafka;

import br.com.kafka.ecommerce.kafka.consumer.ConsumerMessage;
import br.com.kafka.ecommerce.kafka.producer.ProducerMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;

@Slf4j
@RestController
@RequestMapping("/")
public class Controller {

    @Autowired
    private ProducerMessage producerMessage;

    @Autowired
    private ConsumerMessage consumerMessage;

    @GetMapping("envia")
    public ResponseEntity<String> send() throws ExecutionException, InterruptedException {
        producerMessage.sendMessage();
        log.info("Message send success");
        return ResponseEntity.ok().build();
    }

    @GetMapping("consome")
    public ResponseEntity<String> consumer() throws ExecutionException, InterruptedException {
        consumerMessage.consumerMessage();
        log.info("Message consumer success");
        return ResponseEntity.ok().build();
    }

}
