package jax.spring.kafka.config;

import jax.spring.kafka.model.Greeting;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    @Value(value = "${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    private ConsumerFactory<String, String> consumerFactory(String groupId) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(config);
    }

    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(String groupId) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory(groupId));
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> fooKafkaListenerContainerFactory() {
        return kafkaListenerContainerFactory("foo");
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> barKafkaListenerContainerFactory() {
        return kafkaListenerContainerFactory("bar");
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> headersKafkaListenerContainerFactory() {
        return kafkaListenerContainerFactory("headers");
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> partitionsKafkaListenerContainerFactory() {
        return kafkaListenerContainerFactory("partitions");
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> longMessageKafkaListenerContainerFactory() {
        return kafkaListenerContainerFactory("longMessage");
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> filterKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = kafkaListenerContainerFactory("filter");
        factory.setRecordFilterStrategy(record -> record.value().contains("World"));
        return factory;
    }

    private ConsumerFactory<String, Greeting> greetingConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "greeting");
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new JsonDeserializer<>(Greeting.class));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Greeting> greetingKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Greeting> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(greetingConsumerFactory());
        return factory;
    }
}
