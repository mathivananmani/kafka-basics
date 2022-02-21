package Consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class KafkaConsumerConfiguration {
        private static final String BOOTSTRAP_SERVER_URL = "localhost:9092";
    public static Properties getDefaultConsumerProperties(String consumerGroupName) {
        Properties kafkaConsumerProperties = new Properties();
        kafkaConsumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_URL);
        kafkaConsumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaConsumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaConsumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        kafkaConsumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,consumerGroupName);
        return kafkaConsumerProperties;
    }

    public static Properties getAtleastOnceSemanticProperties(String consumerGroupName) {
        Properties kafkaConsumerProperties = new Properties();
        kafkaConsumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_URL);
        kafkaConsumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaConsumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaConsumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        kafkaConsumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,consumerGroupName);
        //Will commit the offset manually after processing the records. This helps to make sure, records are processed atleast once
            //to avoid reprocessing the record, needs to make idempotent consumer manually by assigning id for each record manually
        kafkaConsumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // Maximum number of records fetched in each poll is 500 by default.
        kafkaConsumerProperties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        return kafkaConsumerProperties;
    }
}
;
