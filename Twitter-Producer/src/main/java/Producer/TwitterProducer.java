package Producer;

import Client.TwitterHBClient;
import com.google.common.collect.Lists;
import com.twitter.hbc.core.Client;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    /*
        1. Uses twitter's stream API internally to stream the twitters
        https://stream.twitter.com/1.1/statuses/filter.json?track=twitter
        2. Since essential access has only V2 API access, hence raised elevation access
        2. Establishing a connection to the streaming APIs means making a very long lived HTTPS request, and parsing the response incrementally.
            When connecting to the sampled stream endpoint, you should form a HTTPS request and consume the resulting stream for as long as is practical
     */

    /*
    kafka-topics --create --zookeeper localhost:2181 --topic twitter_tweets --partitions 3 --replication-factor=1
WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both.
Created topic twitter_tweets.
     */
    private static Logger log = LoggerFactory.getLogger(TwitterProducer.class);
    public static void main(String[] args) {

        /* Kafka Producer setup : Start */
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(KafkaProducerConfiguratoin.getHighThroughputClientProperties());
        String twitterTweetsTopic = "twitter_tweets";

        /* Twitter client setup : Start */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(15);
        List<String> termsToSearch = Lists.newArrayList("crypto", "cricket");
        Client hosebirdClient = new TwitterHBClient().getHoseBirdClient(msgQueue, termsToSearch);
        // Attempts to establish a connection.
        hosebirdClient.connect();
        //Now, msgQueue will start being filled with messages

        while (!hosebirdClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(1000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                log.error("Error while polling Queue = "+e);
            }
            if (msg!=null){
                log.info("Processing Message = "+msg);
                kafkaProducer.send(new ProducerRecord<String, String>(twitterTweetsTopic, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if(exception!=null){
                            log.error("Error while producing record = "+exception);
                        }else{
                            log.info("Parition = "+metadata.partition()+"\t Offset = "+metadata.offset());
                        }
                    }
                });
            }

        }

        // add a shutdown hook. This helps to close HBC client & KafkaProducer.
           //Closing Kafka producer will push the pending records in batch to partition
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("stopping application...");
            log.info("shutting down client from twitter...");
            hosebirdClient.stop();
            log.info("closing producer...");
            kafkaProducer.close();
            log.info("done!");
        }));
    }


}
