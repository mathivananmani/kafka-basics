package Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    static Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

    /*
        1. Create consumer properties
        2. Create consumer
        3. Subscribe consumer for topic
        4. Poll for new data
     */

    /*
        Whenever new consumer is added into the consumer group rebalance happens as below

    * removed one parition from existing consumer
        [main] INFO org.apache.kafka.clients.consumer.internals.AbstractCoordinator - [Consumer clientId=consumer-first-application-1, groupId=first-application] Attempt to heartbeat failed since group is rebalancing
        [main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-first-application-1, groupId=first-application] Revoke previously assigned partitions firsttopic-0, firsttopic-2, firsttopic-1
        [main] INFO org.apache.kafka.clients.consumer.internals.AbstractCoordinator - [Consumer clientId=consumer-first-application-1, groupId=first-application] (Re-)joining group
        [main] INFO org.apache.kafka.clients.consumer.internals.AbstractCoordinator - [Consumer clientId=consumer-first-application-1, groupId=first-application] Successfully joined group with generation Generation{generationId=2, memberId='consumer-first-application-1-9e020298-17cf-48f7-8c76-99e616849480', protocol='range'}
        [main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-first-application-1, groupId=first-application] Finished assignment for group at generation 2: {consumer-first-application-1-b4736c8f-be65-4020-a2be-99b1c96e7af8=Assignment(partitions=[firsttopic-2]), consumer-first-application-1-9e020298-17cf-48f7-8c76-99e616849480=Assignment(partitions=[firsttopic-0, firsttopic-1])}
        [main] INFO org.apache.kafka.clients.consumer.internals.AbstractCoordinator - [Consumer clientId=consumer-first-application-1, groupId=first-application] Successfully synced group in generation Generation{generationId=2, memberId='consumer-first-application-1-9e020298-17cf-48f7-8c76-99e616849480', protocol='range'}
        [main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-first-application-1, groupId=first-application] Notifying assignor about the new Assignment(partitions=[firsttopic-0, firsttopic-1])
        ** [main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-first-application-1, groupId=first-application] Adding newly assigned partitions: firsttopic-0, firsttopic-1
        [main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-first-application-1, groupId=first-application] Setting offset for partition firsttopic-0 to the committed offset FetchPosition{offset=28, offsetEpoch=Optional[0], currentLeader=LeaderAndEpoch{leader=Optional[DESKTOP-B8D2M9I:9092 (id: 0 rack: null)], epoch=0}}
        [main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-first-application-1, groupId=first-application] Setting offset for partition firsttopic-1 to the committed offset FetchPosition{offset=24, offsetEpoch=Optional[0], currentLeader=LeaderAndEpoch{leader=Optional[DESKTOP-B8D2M9I:9092 (id: 0 rack: null)], epoch=0}}

    * new consumer got 1 partition
       [main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-first-application-1, groupId=first-application] Setting offset for partition firsttopic-2 to the committed offset FetchPosition{offset=38, offsetEpoch=Optional[0], currentLeader=LeaderAndEpoch{leader=Optional[DESKTOP-B8D2M9I:9092 (id: 0 rack: null)], epoch=0}}

     */
    public static void main(String[] args) {
        //kafka-console-consumer --bootstrap-server localhost:9092 --topic firsttopic --group firstgrp
        String readFromFirstForNewGroup = "earliest";
        String readFromLatestForNewGroup = "latest";//automatically reset the offset to the latest offset. This is default
        String applicationGroupName="first-application";
        String topic="firsttopic";

        //create consumer properties --> consumer group is set here
        Properties kafkaConsumerProperties = KafkaConsumerProperties.getProperties(applicationGroupName, readFromFirstForNewGroup);
        //create consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(kafkaConsumerProperties);

        kafkaConsumer.subscribe(Arrays.asList(topic));

        //polling
        while (true) {
            //consumer records
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> consumerRecord : consumerRecords){
                logger.info("partition = "+consumerRecord.partition() + "\t offset = "+consumerRecord.offset());
                logger.info("key = "+consumerRecord.key()+"\t value = "+consumerRecord.value());
                logger.info("topic = "+consumerRecord.topic());
                logger.info("==========================================");
            }
        }

    }
}
