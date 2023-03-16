package jax.spring.kafka;

import jax.spring.kafka.consumer.KafkaConsumer;
import jax.spring.kafka.model.Greeting;
import jax.spring.kafka.producer.KafkaProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@SpringBootApplication
public class KafkaApplication {

	public static void main(String[] args) throws InterruptedException {
		ConfigurableApplicationContext context = SpringApplication.run(KafkaApplication.class, args);

		KafkaProducer producer = context.getBean(KafkaProducer.class);
		KafkaConsumer consumer = context.getBean(KafkaConsumer.class);

		producer.sendMessage("Hello, World!");
		consumer.getLatch().await(20, TimeUnit.SECONDS);

		/*
		 * Sending message to a topic with 5 partitions,
		 * each message to a different partition. But as per
		 * listener configuration, only the messages from
		 * partition 0 and 3 will be consumed.
		 */
		for (int j = 0; j < 2; j++) {
			for (int i = 0; i < 3; i++) {
				producer.sendMessageToPartition("Hello To Partitioned Topic partition #" + i + "!", i);
			}
		}
		consumer.getPartitionLatch().await(40, TimeUnit.SECONDS);

		producer.sendGreetingMessage(new Greeting("Greetings", "World!"));
		consumer.getGreetingLatch().await(60, TimeUnit.SECONDS);

		context.close();
	}
}
