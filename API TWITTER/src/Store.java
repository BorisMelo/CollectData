import java.net.UnknownHostException;
import java.util.List;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.ResponseList;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

public class Store extends Thread{
   
	static final String hashTag="#NBA";
    static final int count = 100;
    static long sinceId = 0;
    static long numberOfTweets = 0;
   
    public static BasicDBObject recoltTweets(DBCollection collection) {
    	
    	BasicDBObject document;
    	ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
          .setOAuthConsumerKey("zGCw6c3JCmi2nNpbNAKiw3yQo")
          .setOAuthConsumerSecret("KRqUhQSHQS5BRqjS4o4n7BHWGiwj7QysWBJEEJAEG5KLzP9OSn")
          .setOAuthAccessToken("3367577051-D7td8bLutmZYNX5DDCl35bqIb6lcV4947EhdCRE")
          .setOAuthAccessTokenSecret("Yx5YzcCuf2KC0NEIfVGKFCexk9v7cxSbiHypv9gxNLnLf");
        TwitterFactory tf = new TwitterFactory(cb.build());
        Twitter twitter = tf.getInstance();
 
        //get latest tweets as of now
        //At this point store sinceId in database
            Query queryMax = new Query(hashTag);
            queryMax.setCount(count);
            document = getTweets(queryMax, twitter, "maxId", collection);
            if(document.isEmpty()) {
				  System.out.println("Document is EMPTY");
			  }else {
				System.out.println("Document is NOT EMPTY");
			  }
            queryMax = null;
            
            return document;
    }
 
    private static BasicDBObject getTweets(Query query, Twitter twitter, String mode, DBCollection collection) {
    	BasicDBObject document = new BasicDBObject();
    	
        boolean getTweets=true;
        long maxId = 0;
        long whileCount=0;
 
        //while (getTweets){
        int cpt = 0;
        while(cpt<50) {
        	cpt++;
            try {
                QueryResult result = twitter.search(query);

                    int forCount=0;
                    for (Status status: result.getTweets()) {
                        if(whileCount == 0 && forCount == 0){
                            sinceId = status.getId();//Store sinceId in database             
                        }
                        BasicDBObject documentDetail = new BasicDBObject();
                        documentDetail.put("name_tweeter", status.getUser().getName());
                        documentDetail.put("tweet", status.getText());
                        collection.insert(documentDetail);
                        System.out.println("@" + status.getUser().getScreenName() + " : "+status.getUser().getName()+"--------"+status.getText());
                        if(forCount == result.getTweets().size()-1){
                            maxId = status.getId();
                        }
                        forCount++;
                    }
                    numberOfTweets=numberOfTweets+result.getTweets().size();
                    query.setMaxId(maxId-1); 
                //}
            }catch (TwitterException te) {
                System.out.println("Couldn't connect: " + te);
                System.exit(-1);
            }catch (Exception e) {
                System.out.println("Something went wrong: " + e);
                System.exit(-1);
            }
            whileCount++;
        }
        System.out.println("Total tweets count======="+numberOfTweets);
        
        return document;
    }
    
	
	public static void main(String[] args)  {
   	
		Mongo mongo = new Mongo("localhost", 27017);
		DB db = mongo.getDB("nbaDB");
		
		DBCollection table = db.getCollection("tweets");
		
			try {
			  BasicDBObject tweetsDoc = recoltTweets(table);
			  if(tweetsDoc.isEmpty()) {
				  System.out.println("tweetsDoc is EMPTY");
			  }else {
  				System.out.println("tweetsDoc is NOT EMPTY");
  			  }
			  System.out.println("size " +tweetsDoc.size());
		
		    }catch (Exception e) {
		    	e.printStackTrace();
		    }
    }

}