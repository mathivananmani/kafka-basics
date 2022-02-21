package Service;

import Modal.Tweet;

import java.util.List;

public class TweetsService {
    TweetReader tweetReader = new TweetReaderFromFile();
    public List<Tweet> fetchTweets(List<String> tweetIDs){
        List<Tweet> tweets = tweetReader.readTweets(tweetIDs);
        return tweets;
    }
}
