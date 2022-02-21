package Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;

public class TweetConsumer {
    static Logger logger = LoggerFactory.getLogger(TweetConsumer.class);

    public static void main(String[] args) {
        /*
            1.  Create Consumer properties --> Group ID
            2.  Create Consumer client with properties
            3.  Subscribe for Topic
            4.  Poll topics

         */
        String consumerGroup = "tweetLookup";
        String twitterTweetsTopic = "tweets";
        String readFromFirstForNewGroup = "earliest";
        String readFromLatestForNewGroup = "latest";//automatically reset the offset to the latest offset. This is default

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(
                KafkaConsumerProperties.getProperties(consumerGroup, readFromFirstForNewGroup)
        );
        consumer.subscribe(Arrays.asList(twitterTweetsTopic));

        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord consumerRecord : consumerRecords) {
                logger.info("partition == " + consumerRecord.partition() + "\t offset ==" + consumerRecord.offset());
                logger.info("Key == " + consumerRecord.key() + "Value == " + consumerRecord.value());
                logger.info("Headers == " + consumerRecord.headers());
            }
        }
    }
}
