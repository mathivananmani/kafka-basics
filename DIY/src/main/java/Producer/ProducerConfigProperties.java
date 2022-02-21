package Producer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerConfigProperties {


    public static final String BOOTSTRAP_SERVER_ADDRESS = "localhost:9092";

    public static Properties getDefaultProperties() {
        Properties kafkaProducerProperties = new Properties();
        //kafka-console-tutorial.producer --bootstrap-server localhost:9092
        kafkaProducerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER_ADDRESS);
        kafkaProducerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProducerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return kafkaProducerProperties;
    }
}
