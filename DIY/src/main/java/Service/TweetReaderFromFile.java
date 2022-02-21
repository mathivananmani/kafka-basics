package Service;

import Modal.Tweet;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileReader;
import java.io.IOException;
import java.util.List;

public class TweetReaderFromFile implements TweetReader{
    JSONParser parser = new JSONParser();

    public List<Tweet> readTweets(List<String> tweetIDs){
        List<Tweet> tweetLists =null;
        try {
            //URL url = getClass().getResource("./resources/TweetLookUp.json");
           // System.out.println("Path = "+url.getPath());

           //JSONParser helps to read the JSON file and create a JSON string
            Object obj = parser.parse(new FileReader("D:\\Mathi\\Learning\\Kafka\\latest\\Twitter-Project\\DIY\\src\\main\\resources\\TweetLookUp.json"));
            JSONObject jsonObject =  (JSONObject) obj;
            String jsonString = jsonObject.get("data").toString();
            System.out.println("jsonString = "+jsonString);

            //Object Mapper from Jackson helps to convert JSON string to Java Object
            ObjectMapper objectMapper = new ObjectMapper();
            tweetLists = objectMapper.readValue(jsonString,new TypeReference<List<Tweet>>(){});
            tweetLists.stream().forEach(t-> System.out.println(t.getId()+"  "+t.getText()));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return tweetLists;
    }
}
