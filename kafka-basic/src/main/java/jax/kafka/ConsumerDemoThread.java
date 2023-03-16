package jax.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoThread {
    public static void main(String[] args) {
        new ConsumerDemoThread().run();
    }

    private ConsumerDemoThread() {

    }

    private void run() {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoThread.class);
        CountDownLatch latch = new CountDownLatch(1);

        logger.info("Creating the consumer thread");
        ConsumerRunnable consumerRunnable = new ConsumerRunnable(
                "127.0.0.1:9092",
                "demo-app",
                "first-topic",
                latch
        );

        Thread consumerThread = new Thread(consumerRunnable);
        consumerThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            consumerRunnable.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.error("Application get interrupted", e);
            }
            logger.info("Application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application get interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {

        private final Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

        private CountDownLatch latch;

        private KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(String bootstrapServers, String groupId, String topic, CountDownLatch latch) {
            this.latch = latch;

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            this.consumer = new KafkaConsumer<>(properties);
            this.consumer.subscribe(Collections.singleton(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: {}, Value: {} ", record.key(), record.value());
                        logger.info("Partition: {}, Offset: {}", record.partition(), record.offset());
                    }

                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal!");
            } finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown() {
            consumer.wakeup();
        }
    }
}
