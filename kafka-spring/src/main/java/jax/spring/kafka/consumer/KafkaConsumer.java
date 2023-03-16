package jax.spring.kafka.consumer;

import jax.spring.kafka.model.Greeting;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.concurrent.CountDownLatch;

@Service
public class KafkaConsumer {

    private CountDownLatch latch = new CountDownLatch(3);

    private CountDownLatch partitionLatch = new CountDownLatch(3);

    private CountDownLatch filterLatch = new CountDownLatch(2);

    private CountDownLatch greetingLatch = new CountDownLatch(1);

    @KafkaListener(topics = "${spring.kafka.topic}", groupId = "foo", containerFactory = "fooKafkaListenerContainerFactory")
    public void listenGroupFoo(String message) {
        System.out.println("Received message in group foo: " + message);
        latch.countDown();
    }

    @KafkaListener(topics = "${spring.kafka.topic}", groupId = "bar", containerFactory = "barKafkaListenerContainerFactory")
    public void listenGroupBar(String message) {
        System.out.println("Received message in group bar: " + message);
        latch.countDown();
    }

    @KafkaListener(topics = "${spring.kafka.topic}", containerFactory = "headersKafkaListenerContainerFactory")
    public void listenWithHeaders(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        System.out.println("Received message in group headers: " + message
                            + " from partition: " + partition);
        latch.countDown();
    }

    @KafkaListener(topicPartitions = @TopicPartition(topic = "${spring.kafka.topic}", partitions = {"0", "2"}), containerFactory = "partitionsKafkaListenerContainerFactory")
    public void listenToPartition(@Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        System.out.println("ListenToPartition Received Message: " + message + " from partition: " + partition);
        this.partitionLatch.countDown();
    }

    @KafkaListener( topics = "${spring.kafka.topic}", containerFactory = "filterKafkaListenerContainerFactory")
    public void listenWithFilter(String message) {
        System.out.println("Received Message in filtered listener: " + message);
    }

    @KafkaListener(topics = "${spring.kafka.greeting.topic}", containerFactory = "greetingKafkaListenerContainerFactory")
    public void greetingListener(Greeting greeting) {
        System.out.println("Received greeting message: " + greeting);
        this.greetingLatch.countDown();
    }

    public CountDownLatch getLatch() {
        return latch;
    }

    public CountDownLatch getPartitionLatch() {
        return partitionLatch;
    }

    public CountDownLatch getFilterLatch() {
        return filterLatch;
    }

    public CountDownLatch getGreetingLatch() {
        return greetingLatch;
    }
}
