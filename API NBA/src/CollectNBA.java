import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.io.InputStream;

import org.bson.Document;
import org.json.*;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

import com.mongodb.*;

public class CollectNBA extends Thread{
	
	//Periode avant la collecte des donnees en ms
	static int periode = 86400000;
	
	
	
	public static BasicDBObject getTeams(DBCollection collection) {
	        
		
			BasicDBObject document = new BasicDBObject();
	        //Map<Integer, String> teams = new HashMap<>();
	        Map<Map<String, Integer>, Map<String, String>> teams = new HashMap<>();
	        String result = "";
	        
	        try {
	            String myurl= "http://data.nba.net";
	
	            URL url = new URL(myurl);
	            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
	            connection.connect();
	            InputStream inputStream = connection.getInputStream();
	            result = InputStreamOperations.InputStreamToString(inputStream);
	            
	            // On r�cup�re le JSON complet
	            JSONObject jsonObject = new JSONObject(result);
	           
	            //Recuperer tableau des noms des team et id
	            String name;
	            int id_team;
	            JSONArray teamList = jsonObject.getJSONObject("sports_content").getJSONObject("teams").getJSONArray("team");
	            for (int i=0; i<teamList.length(); i++) {
	            	id_team = teamList.getJSONObject(i).getInt("team_id");
	            	name = teamList.getJSONObject(i).getString("team_name");
	            	
	            	if(teamList.getJSONObject(i).getBoolean("is_nba_team") == true) {
	            		//teams.put(id_team, name);
	            		//teams.put(team_id, team_name);
	            		BasicDBObject documentDetail = new BasicDBObject();
		            	documentDetail.put("id_team", id_team);
		            	documentDetail.put("name_team", name);
		            	collection.insert(documentDetail);
		            	//document.put("team "+i, documentDetail);
		            
	            	}
	            }

	            
	        } catch (Exception e) {
	            e.printStackTrace();
	        } 
	        
	        return document;
	    }
	
		
		public static BasicDBObject getScores(DBCollection collection) {
		
			BasicDBObject document = new BasicDBObject();
			String linkScore = "";
	        //Map<Integer, Map<Integer, String>> scoresDay = new HashMap<>();
			Map<Map<String, Integer>, Map<Map<String, Integer>, Map<String, String>>> scoresDay = new HashMap<>();
	        String result = "";
	        
	      //LINK TO SCORES
	        try {
	            String myurl= "http://data.nba.net/10s/prod/v1/today.json";
	
	            URL url = new URL(myurl);
	            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
	            connection.connect();
	            InputStream inputStream = connection.getInputStream();
	            result = InputStreamOperations.InputStreamToString(inputStream);
	            
	            JSONObject jsonObject = new JSONObject(result);
	            linkScore = jsonObject.getJSONObject("links").getString("todayScoreboard");
	
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
	
	        //SCORE DES MATCHS DU JOURS
	        try {
	            String myurl= "http://data.nba.net/10s" +linkScore;
	            System.out.println(myurl);
	
	            URL url = new URL(myurl);
	            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
	            connection.connect();
	            InputStream inputStream = connection.getInputStream();
	            result = InputStreamOperations.InputStreamToString(inputStream);
	            
	            // On r�cup�re le JSON complet
	            JSONObject jsonObject = new JSONObject(result);
	         
	            //Recuperer tableau id des games et score de chaque team
	            int id_game;
	            int id_team1, id_team2;
	            String score_team1;
	            String score_team2;
	            JSONArray gameList = jsonObject.getJSONArray("games");
	            for (int i=0; i<gameList.length(); i++) {
	            	id_game = gameList.getJSONObject(i).getInt("gameId");
	            	id_team1 = gameList.getJSONObject(i).getJSONObject("vTeam").getInt("teamId");
	            	id_team2 = gameList.getJSONObject(i).getJSONObject("hTeam").getInt("teamId");
	            	score_team1 = gameList.getJSONObject(i).getJSONObject("vTeam").getString("score");
	            	score_team2 = gameList.getJSONObject(i).getJSONObject("hTeam").getString("score");
	            	
	            	BasicDBObject documentDetail = new BasicDBObject();
	            	//documentDetail.put("id_game", id_game);
	            	documentDetail.put("id_team1", id_team1);
	            	documentDetail.put("score_team1", score_team1);
	            	documentDetail.put("id_team2", id_team2);
	            	documentDetail.put("score_team2", score_team2);
	            	collection.insert(documentDetail);
	            	//document.put(""+id_game, documentDetail);
	            
	            }
	            
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
	        
			//return result;
	        return document;
		}

	public static void main(String[] args){
		
		
		//Connexion � la base mongoDB
		Mongo mongo = new Mongo("localhost", 27017);
		DB db = mongo.getDB("nbaDB");
		DBCollection collectionScores, collectionTeams;
		 
		while(true) {	
			
			try {
				
				collectionScores = db.getCollection("scores");
	            collectionTeams = db.getCollection("teams");
				
				System.out.println("Debut de la collecte ...");
				getTeams(collectionTeams);
				getScores(collectionScores);
	
	            System.out.println("Done !");
	            
	        } catch (Exception e) {
	            e.printStackTrace();
	        } 
			try {
				sleep(periode);
			} catch(InterruptedException e) {	
			}
		}
	}
}
