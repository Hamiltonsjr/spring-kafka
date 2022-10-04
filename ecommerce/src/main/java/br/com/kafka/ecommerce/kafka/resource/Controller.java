package br.com.kafka.ecommerce.kafka.resource;

import br.com.kafka.ecommerce.kafka.consumer.ConsumerEmail;
import br.com.kafka.ecommerce.kafka.consumer.ConsumerOrder;
import br.com.kafka.ecommerce.kafka.producer.ProducerMessage;
import br.com.kafka.ecommerce.kafka.service.LogService;
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
    private ConsumerOrder consumerMessage;

    @Autowired
    private ConsumerEmail email;

    @Autowired
    private LogService logService;

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

    @GetMapping("email")
    public ResponseEntity<String> consumerEmail() throws ExecutionException, InterruptedException {
        email.consumerEmail();
        log.info("Message consumer success");
        return ResponseEntity.ok().build();
    }

    @GetMapping("log")
    public ResponseEntity<String> consumerLog() throws ExecutionException, InterruptedException {
        logService.logConsumer();
        log.info("Message consumer success");
        return ResponseEntity.ok().build();
    }
}
