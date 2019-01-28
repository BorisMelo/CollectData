import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.io.InputStream;
import org.json.*;
import com.mongodb.DB;
import com.mongodb.util.JSON;
import com.mongodb.*;

public class CollectNBA extends Thread{
	
	//Periode avant la collecte des donnees en ms
	static int periode = 6000;
	
	public static String getTeams() {
	        
	        Map<Integer, String> teams = new HashMap<>();
	        String result = "";
	        
	        try {
	            String myurl= "http://data.nba.net";
	
	            URL url = new URL(myurl);
	            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
	            connection.connect();
	            InputStream inputStream = connection.getInputStream();
	            result = InputStreamOperations.InputStreamToString(inputStream);
	            
	            // On récupère le JSON complet
	            JSONObject jsonObject = new JSONObject(result);
	           
	            //Recuperer tableau des noms des team et id
	            String name;
	            int id_team;
	            JSONArray teamList = jsonObject.getJSONObject("sports_content").getJSONObject("teams").getJSONArray("team");
	            for (int i=0; i<teamList.length(); i++) {
	            	id_team = teamList.getJSONObject(i).getInt("team_id");
	            	name = teamList.getJSONObject(i).getString("team_name");
	            	if(teamList.getJSONObject(i).getBoolean("is_nba_team") == true) {
	            		teams.put(id_team, name);
	            	}
	            }
	            
	            /*//Afficher la liste des team name et id
	            System.out.println("AFFICHAGE DES ID ET NOM DES TEAMS \n");
	            Set<Entry<Integer, String>> setTeams = teams.entrySet();
	            Iterator<Entry<Integer, String>> it = setTeams.iterator();
	            while(it.hasNext()){
	                Entry<Integer, String> e = it.next();
	                System.out.println(e.getKey() + " : " + e.getValue());
	            }*/
	            
	            //Convertir le map en json string
	            JSONObject jo = new JSONObject(teams);
	            result = jo.toString();
	            System.out.println(result);
	     
	        } catch (Exception e) {
	            e.printStackTrace();
	        } 
	        
	        return result;
	    }
	
		
		public static String getScores() {
		
			String linkScore = "";
	        Map<Integer, Map<Integer, String>> scoresDay = new HashMap<>();
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
	            
	            // On récupère le JSON complet
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
	            	Map<Integer, String> teamScore = new HashMap<>();
	            	teamScore.put(id_team1, score_team1);
	            	teamScore.put(id_team2, score_team2);
	            	scoresDay.put(id_game, teamScore);
	            }
	            
	            /*//Afficher la liste des team name et id
	            System.out.println("AFFICHAGE DES SCORES DES MATCHS DU JOUR \n");
	            Set<Entry<Integer, Map<Integer, String>>> setScoresDay = scoresDay.entrySet();
	            Iterator<Entry<Integer, Map<Integer, String>>> it = setScoresDay.iterator();
	            while(it.hasNext()){
	                Entry<Integer, Map<Integer, String>> e = it.next();
	                System.out.println(e.getKey() + " : " + e.getValue());
	             }*/
	            
	            //Convertir le map en json string
	            JSONObject jo = new JSONObject(scoresDay);
	            result = jo.toString();
	            System.out.println(result);
	
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
	        
			return result;
		}

	
	public static void main(String[] args){
		while(true) {	
			System.out.println("Debut de la collecte ...");
			String teamsJson = getTeams();
			String scoresJson = getScores();
			
			try {
				//Connexion à la base mongoDB
	            Mongo mongo = new Mongo("localhost", 27017);
	            DB db = mongo.getDB("dbName");
	            DBCollection collection = db.getCollection("collectionName");
	
	            // convert JSON to DBObject directly
	            DBObject objTeams = (DBObject) JSON.parse("teamsJson");
	            DBObject objScores = (DBObject) JSON.parse("scoresJson");
	            
	            //Insertion dans mongoDB
	            collection.insert(objTeams);
	            collection.insert(objScores);
	
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
