package Service;

import Modal.Tweet;

import java.util.List;

public interface TweetReader {

    public List<Tweet> readTweets(List<String> tweetIDs);
}
