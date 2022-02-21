package Producer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerConfiguratoin {


    private static final String BOOTSTRAP_SERVER_ADDRESS = "localhost:9092";

    public static Properties getDefaultClientProperties() {
        Properties kafkaProducerProperties = new Properties();
        //kafka-console-tutorial.producer --bootstrap-server localhost:9092
        kafkaProducerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER_ADDRESS);
        kafkaProducerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProducerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return kafkaProducerProperties;
    }

    public static Properties getHighThroughputClientProperties() {
        Properties kafkaProducerProperties = new Properties();
        //kafka-console-tutorial.producer --bootstrap-server localhost:9092
        kafkaProducerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER_ADDRESS);
        kafkaProducerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProducerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //Settingup compression type & batch params
        kafkaProducerProperties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        kafkaProducerProperties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        kafkaProducerProperties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32 KB batch size
        return kafkaProducerProperties;
    }
}
