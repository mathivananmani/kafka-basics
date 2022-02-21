package Client;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.List;
import java.util.concurrent.BlockingQueue;

public class TwitterHBClient {

    private static final String consumerKey = "zsJNzydtHIPeGn2VN1WyZTGYg";
    private static final String consumerSecret = "ErhvPil4ITmyes1QhKVVUVkUs4qL5xLHQRI7yEmcVlSb9mgXZH";
    private static final String token = "140953415-l0iYFwmMpjz7ytmcLhl6FEMvXj3c5DBkO4I4tVdJ";
    private static final String secret = "4FEFJZoQvibEODWJ7VU7AqQXncBpaYmXgrudWhMKRwng0";

    public Client getHoseBirdClient(BlockingQueue<String> msgQueue, List<String> terms){

        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        hosebirdEndpoint.trackTerms(terms);
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
        //.eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events
        return builder.build();
    }
}