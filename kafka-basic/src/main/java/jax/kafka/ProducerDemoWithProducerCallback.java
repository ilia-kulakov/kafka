package jax.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithProducerCallback {

    public static final Logger LOG = LoggerFactory.getLogger(ProducerDemoWithProducerCallback.class);
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        LOG.info("Sending messages");
        String[] months = new String[] {"January", "February", "March", "April", "May"};
        for (int i = 0; i < months.length; i++) {
            String topic = "first-topic";
            String value = String.format("What a wonderful %s!", months[i]);
            String key = "id_" + ((i+1) % 2 + 1);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        LOG.info("Received new metadata. Topic: {} Partition: {} Offset: {} Timestamp: {}",
                                metadata.topic(),
                                metadata.partition(),
                                metadata.offset(),
                                metadata.timestamp()
                        );

                    } else {
                        LOG.error("Error has occurred", exception);
                    }
                }
            });
        }
        LOG.info("Message was sent");
        producer.flush();
        producer.close();
        LOG.info("Finish");
    }

}