import twitter4j.*;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;


public class Twitt {
    static final String hashTag="#NBA";
    static final int count = 100;
    static long sinceId = 0;
    static long numberOfTweets = 0;
 
    public static void main(String[] args){
    
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
            getTweets(queryMax, twitter, "maxId");
            queryMax = null;
 
            //get tweets that may have occurred while processing above data
            //Fetch sinceId from database and get tweets, also at this point store the sinceId
            do{
                Query querySince = new Query(hashTag);
                querySince.setCount(count);
                querySince.setSinceId(sinceId);
                getTweets(querySince, twitter, "sinceId");
                querySince = null;
            }while(checkIfSinceTweetsAreAvaliable(twitter));
 
    }
 
    private static boolean checkIfSinceTweetsAreAvaliable(Twitter twitter) {
        Query query = new Query(hashTag);
        query.setCount(count);
        query.setSinceId(sinceId);
        try {
            QueryResult result = twitter.search(query);
            if(result.getTweets()==null || result.getTweets().isEmpty()){
                query = null;
                return false;
            }
        } catch (TwitterException te) {
            System.out.println("Couldn't connect: " + te);
            System.exit (0) ;
            //-1);
        }catch (Exception e) {
            System.out.println("Something went wrong: " + e);
            System.exit(0);
        }
        return true;
    }
 
    private static void getTweets(Query query, Twitter twitter, String mode) {
        boolean getTweets=true;
        long maxId = 0;
        long whileCount=0;
 
        while (getTweets){
            try {
                QueryResult result = twitter.search(query);
                if(result.getTweets()==null || result.getTweets().isEmpty()){
                    getTweets=false;
                }else{
                    System.out.println("");
                    System.out.println("Gathered " + result.getTweets().size() + " tweets");
                    int forCount=0;
                    for (Status status: result.getTweets()) {
                        if(whileCount == 0 && forCount == 0){
                            sinceId = status.getId();//Store sinceId in database
                            System.out.println("sinceId= "+sinceId);
                        }
                        System.out.println("Id= "+status.getId());
                        System.out.println("@" + status.getUser().getScreenName() + " : "+status.getUser().getName()+"--------"+status.getText());
                        if(forCount == result.getTweets().size()-1){
                            maxId = status.getId();
                            System.out.println("maxId= "+maxId);
                        }
                        System.out.println("");
                        forCount++;
                    }
                    numberOfTweets=numberOfTweets+result.getTweets().size();
                    query.setMaxId(maxId-1); 
                }
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
    }   
}