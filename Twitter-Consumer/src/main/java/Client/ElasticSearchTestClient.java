package Client;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ElasticSearchTestClient {
    private static final Logger log = LoggerFactory.getLogger(ElasticSearchTestClient.class);
    public static void main(String[] args) throws IOException {
        RestHighLevelClient esRestClient = ElasticSearchClient.createClient();
        String documentToIndex = "{\"foo\":\"bar\"}";
        //removed type as it is deprecated from 7x
        //Index needs to be created before sending documents else will get exception "[{"type":"index_not_found_exception","reason":"no such index [tweets]"
        IndexRequest indexRequet = new IndexRequest("tweets")
                .source(documentToIndex, XContentType.JSON);
        IndexResponse indexResponse = esRestClient.index(indexRequet, RequestOptions.DEFAULT);
        log.info("IndexResponse ID == "+indexResponse.getId());
        log.info("IndexResponse toString == "+indexResponse.toString());
        /*

        [main] INFO Client.ElasticSearchTestClient - IndexResponse toString == IndexResponse[index=tweets,type=_doc,id=ZD0uDH8BYSNvjPY0PMow,version=1,result=created,seqNo=0,primaryTerm=1,shards={"total":2,"successful":2,"failed":0}]
         */
        log.info("IndexResponse getIndex == "+indexResponse.getIndex());
        //log.info("IndexResponse getLocation == "+indexResponse.getLocation().toString());
        esRestClient.close();
    }
}
