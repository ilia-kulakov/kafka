package jax.spring.kafka.producer;

import jax.spring.kafka.model.Greeting;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Service
public class KafkaProducer {
    private KafkaTemplate<String, String> kafkaTemplate;
    private KafkaTemplate<String, Greeting> greetingKafkaTemplate;

    @Value(value = "${spring.kafka.topic}")
    private String topicName;

    @Value(value = "${spring.kafka.greeting.topic}")
    private String greetingTopicName;

    @Autowired
    public KafkaProducer(KafkaTemplate<String, String> kafkaTemplate, KafkaTemplate<String, Greeting> greetingKafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        this.greetingKafkaTemplate = greetingKafkaTemplate;
    }

    public void sendMessage(String message) {
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, message);
        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                System.out.println("Unable to send message=["
                        + message + "] due to : " + ex.getMessage());
            }

            @Override
            public void onSuccess(SendResult<String, String> result) {
                System.out.println("Sent message=[" + message +
                        "] with offset=[" + result.getRecordMetadata().offset() + "]");
            }
        });
    }

    public void sendMessageToPartition(String message, int partition) {
        kafkaTemplate.send(topicName, partition, null, message);
    }

    public void sendGreetingMessage(Greeting greeting) {
        greetingKafkaTemplate.send(greetingTopicName, greeting);
    }
}
