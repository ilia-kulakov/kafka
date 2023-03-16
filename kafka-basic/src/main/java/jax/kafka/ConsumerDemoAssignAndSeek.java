package jax.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAssignAndSeek {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignAndSeek.class);

        String bootstrapServers = "127.0.0.1:9092";
        String topic = "first-topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        long offsetToReadFrom = 15L;
        consumer.assign(Collections.singleton(partitionToReadFrom));

        consumer.seek(partitionToReadFrom, offsetToReadFrom);
        int numberOfMessagesToRead = 5;
        int counter = 0;

        while(counter < numberOfMessagesToRead) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> record : records) {
                logger.info("Key: {}, Value: {} ", record.key(), record.value());
                logger.info("Partition: {}, Offset: {}", record.partition(), record.offset());
                counter++;
                if(counter >= numberOfMessagesToRead) {
                    break;
                }
            }
        }
        logger.info("Exiting the application");
    }
}
