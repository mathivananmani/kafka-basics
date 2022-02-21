package Consumer;

import Client.ElasticSearchClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;

public class TwitterConsumer {
    private static final String CONSUMER_GROUP_NAME = "tweets_indexer";
    private static final String TOPIC_NAME = "twitter_tweets";
    private static Logger log = LoggerFactory.getLogger(TwitterConsumer.class);
    private static boolean PUSH_BULK_RECORDS_TO_ES = true;
    public static void main(String[] args) {
        /*
            1.  Create Kafka consumer client with properties
            2.  Subscribe for topic
            3.  Start poll the records
         */

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(
                KafkaConsumerConfiguration.getAtleastOnceSemanticProperties(CONSUMER_GROUP_NAME));
        kafkaConsumer.subscribe(Arrays.asList(TOPIC_NAME));

        RestHighLevelClient esRestClient = ElasticSearchClient.createClient();
        int count = 0;

        while(true){
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
            int totalNumberOfRecordsFetchedInAPoll = consumerRecords.count();
            count = count +1;
            log.info("Poll count == "+count+"\t No of records fetched == "+totalNumberOfRecordsFetchedInAPoll);
            if(totalNumberOfRecordsFetchedInAPoll > 0) {
                if (PUSH_BULK_RECORDS_TO_ES) {
                    pushBulkRecordsToES(consumerRecords, esRestClient);
                } else {
                    pushIndividualRecordsToES(consumerRecords, esRestClient);
                }
            }
            //Adding 1 second pause after sending data to ES, just for testing purpose
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }

    private static void pushBulkRecordsToES(ConsumerRecords<String, String> consumerRecords, RestHighLevelClient esRestClient) {
        BulkRequest bulkRequest = new BulkRequest();
        for(ConsumerRecord<String, String> consumerRecord : consumerRecords){
            String consumerID = getKafkaGenericIdToCreateIdempotentConsumer(consumerRecord);
            //String consumerID = getTweetIdToCreateIdempotentConsumer(consumerRecord);
            //log.info("consumerID == "+consumerID);
            IndexRequest indexRequet = new IndexRequest("tweets")
                    .source(consumerRecord.value(), XContentType.JSON)
                    .id(consumerID);//ES use this ID instead of creating its own unique id. So, if same record is sent again, ES will update the existing record instead of creating new record
            bulkRequest.add(indexRequet);
        }
        BulkResponse bulk = null;
        try {
            bulk = esRestClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("Exception pushing bulk request for indices "+ bulkRequest.getIndices() + " == "+e.getMessage());
        }
        log.info("Bulk response status == "+bulk.status());
    }

    private static void pushIndividualRecordsToES(ConsumerRecords<String, String> consumerRecords, RestHighLevelClient esRestClient) {
        for(ConsumerRecord<String, String> consumerRecord : consumerRecords){
            String consumerID = getKafkaGenericIdToCreateIdempotentConsumer(consumerRecord);
            //String consumerID = getTweetIdToCreateIdempotentConsumer(consumerRecord);
            log.info("consumerID == "+consumerID);
            IndexRequest indexRequet = new IndexRequest("tweets")
                    .source(consumerRecord.value(), XContentType.JSON)
                    .id(consumerID);//ES use this ID instead of creating its own unique id. So, if same record is sent again, ES will update the existing record instead of creating new record
            IndexResponse indexResponse = null;
            try {
                indexResponse = esRestClient.index(indexRequet, RequestOptions.DEFAULT);
            } catch (IOException e) {
                e.printStackTrace();
                log.error("Exception while indexing "+ consumerRecord.value() + " == "+e.getMessage());
            }
            log.info("IndexResponse ID == "+indexResponse.getId());
            log.info("IndexResponse toString == "+indexResponse.toString());
        }
    }

    private static String getTweetIdToCreateIdempotentConsumer(ConsumerRecord<String, String> consumerRecord) {

        return "";
    }

    private static String getKafkaGenericIdToCreateIdempotentConsumer(ConsumerRecord consumerRecord) {
        // Topic Name + Partition Name + Offset
        return TOPIC_NAME+consumerRecord.partition()+consumerRecord.offset();
    }

    /*

    totalNumberOfRecordsFetchedInAPoll == 100
    consumerID == twitter_tweets20
    IndexResponse ID == twitter_tweets20
    IndexResponse toString == IndexResponse[index=tweets,type=_doc,id=twitter_tweets20,version=1,result=created,seqNo=1,primaryTerm=1,shards={"total":2,"successful":2,"failed":0}]
     */
}

