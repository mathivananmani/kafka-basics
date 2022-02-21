package Producer;

import Modal.Tweet;
import Service.TweetsService;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class TweetProducer {
    static Logger logger = LoggerFactory.getLogger(TweetProducer.class);
    public static void main(String[] args) {
        /*
            1. Set tutorial.producer config properties
            2. Create Kafka Producer client with properties
            3. Create Producer Record --> List of tweets
            4. Send Producer records to the Topic
         */

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(ProducerConfigProperties.getDefaultProperties());
        String twitterTweetsTopic = "tweets";

        ProducerRecord<String, String> producerRecord = null;//new ProducerRecord<String, String>();
        String key = null;
        List<String> listOfTweetIDsToBeFetched = Arrays.asList("20","1493510699997286400","1493269005250924544","1493215092187430915");
        List<Tweet> tweetsList = new TweetsService().fetchTweets(listOfTweetIDsToBeFetched);
       // System.out.println("tweetsList"+tweetsList);

        for (int i = 0; i < tweetsList.size(); i++) {
            Tweet tweet = tweetsList.get(i);
            logger.info("Key =="+tweet.getId());
            logger.info("Value =="+tweet);
            key = tweet.getId();
            producerRecord = new ProducerRecord<String, String>(twitterTweetsTopic, key, tweet.toString());
            kafkaProducer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(metadata != null){
                        logger.info("Partition = "+metadata.partition()+"\t Offset = "+metadata.offset());
                        logger.info("metadata = "+metadata.toString());
                    }else{
                        logger.error("Exception while producing record ="+exception.getMessage());
                    }
                }
            });
        }
        kafkaProducer.close();
    }
}
