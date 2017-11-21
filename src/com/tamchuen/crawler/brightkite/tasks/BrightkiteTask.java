package com.tamchuen.crawler.brightkite.tasks;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.swing.JOptionPane;

import org.json.JSONArray;
import org.json.JSONObject;

import com.tamchuen.crawler.Config;
import com.tamchuen.crawler.brightkite.db.BrightkiteDBManager;
import com.tamchuen.crawler.brightkite.domain.Spot;
import com.tamchuen.crawler.brightkite.domain.User;
import com.tamchuen.crawler.brightkite.domain.UserCheckin;
import com.tamchuen.crawler.gowalla.db.GowallaDBManager;
import com.tamchuen.crawler.tasks.AbstractTask;
import com.tamchuen.crawler.util.HttpUtil;
import com.tamchuen.jutil.j4log.Logger;
import com.tamchuen.jutil.string.StringUtil;
import com.tamchuen.jutil.util.FileUtil;
import com.tamchuen.jutil.util.MappedQueue;
import com.tamchuen.jutil.util.Pair;
/**
 * The task for crawling brightkite.com using brightkite API
 * 
 * @author Dequan
 */
public class BrightkiteTask extends AbstractTask{
	public static final String NAME = BrightkiteTask.class.getSimpleName();
	private static final Logger logger = Logger.getLogger( NAME );
	
	public static final String USERS_URL = "http://brightkite.com/people/";
	public static final String SPOTS_URL = "http://brightkite.com/places/";
	public static final String OBJECTS_URL = "http://brightkite.com/objects.json";

	public static final int POLL_INTERVAL = 10000;
	public static final int MAX_RETRY_COUNT =  5;
	public static final int HTTP_TIMEOUT = 10000;
	public static final int SEARCH_RADIUS = 1000;
	public static final int PAGE_SIZE = 100;
	public static final int MAX_PAGE_NUMBER = 500;
	public static final String PERSON_QUEUE_FILE ="brightkite-persons.txt";
	public static final String PLACE_QUEUE_FILE ="brightkite-places.txt";
	
	// we only need cities in 
	// 2011 12/06 top 5 cities
	public static final String TOP_CITIES = "Austin,New York,San Francisco,Seattle,Los Angeles";
	//public static final String TOP_CITIES = "Dallas,Houston,Chicago,San Diego";
	protected  BlockingQueue<String> personQueue;
	protected  BlockingQueue<String> placeQueue;

	private PersonProcessor personProcessor;
	private PlaceProcessor placeProcessor;
	
	public BrightkiteTask() {
		this.setName(NAME);
	}

	/**
	 * init the queue for task IDs (user/spot IDs)
	 */
	public void initialize(Config config){
		super.initialize(config);
		personQueue = new LinkedBlockingQueue<String>(MAX_TASKIDS);
		placeQueue = new LinkedBlockingQueue<String>(MAX_TASKIDS);
	}

	public boolean start(){
		// set status as started
		super.start();
		
        List<String> personsIdList = new ArrayList<String>();
        List<String> placesIdList = new ArrayList<String>();
        int personAddedCount = 0;
        int placeAddedCount = 0;
        
		// check if load id ranges from file
		if( config.isRangeFromFile() ){
			// get queues from files
			String tmpFile = config.getRangeFilePath();
			String outDir = tmpFile.substring(0, tmpFile.lastIndexOf(File.separator));
			File dir = new File(outDir);
		    if(!dir.exists()){
		    	log( NAME + " : dir not exists, " + dir );
		    	return false;	
		    }
		    // check two files under the dir
		    File personsIdFile = new File( outDir + File.separator + PERSON_QUEUE_FILE);
	        File placesIdFile = new File( outDir + File.separator + PLACE_QUEUE_FILE);

	        // check if exist
	        if( personsIdFile.exists() ){
	        	// it's given from a file, get ids from file and add to the queue
	        	personsIdList = FileUtil.readFile2List( personsIdFile.getAbsolutePath(), "UTF-8");
				if( personsIdList == null ){
					statusLog("Get persons id range failed from file : " + personsIdFile.getAbsolutePath()  );
				}
	        }
	        
	        if( placesIdFile.exists() ){
	        	placesIdList = FileUtil.readFile2List( placesIdFile.getAbsolutePath(), "UTF-8");
				if( placesIdList == null ){
					statusLog("Get places id range failed from file : " + placesIdFile.getAbsolutePath()  );
				}
	        }    
		}else{
			// if not load ranges from file, get lat, lng
			String latlng = JOptionPane.showInputDialog(
					"Please input the latitude and longitude, separated by comma.\nEg: 40.7008291704,-74.0130579472",
					"40.7008291704,-74.0130579472");
			if( latlng ==null || latlng.indexOf(",") < 1){
				statusLog(this.getName() + " : latitude and longitude not valid.");
				return false;
			}

			// validation
			String[] latlngs = StringUtil.split(latlng, ",");
			Double lat = StringUtil.convertDouble(latlngs[0], -91);
			Double lng = StringUtil.convertDouble(latlngs[1], -181);

			if( lat > 90 || lat < -90 ){
				statusLog(this.getName() + " : latitude should be from -90 to 90.");
				return false;
			}
			if( lng > 180 || lat < -180 ){
				statusLog(this.getName() + " : longitude should be from -180 to 180.");
				return false;
			}
			// get checkins
			getCheckinsFromLatLng( lat, lng, personsIdList,placesIdList );
		}
		
        if( personsIdList.size() ==0 && placesIdList.size() ==0){
        	statusLog("No ids found." );
        	return false;
        }
        
		// check if need to check duplicate, and add ids to task queue
		if( config.isCheckDuplicateId() ){
			// check DB first
			for(String id : personsIdList ){
				String oldId = BrightkiteDBManager.checkUser( id );
				if( oldId == null ){
					personQueue.offer( id );
					personAddedCount++;
				}
			}
			for(String id : placesIdList ){
				String oldId = BrightkiteDBManager.checkSpot( id );
				if( oldId == null ){
					placeQueue.offer( id );
					placeAddedCount++;
				}
			}
			
		}else{
			// add to task Queue directly
			for(String id : personsIdList ){
				personQueue.offer( id ); 
				personAddedCount ++;
			}
			for(String id : placesIdList ){
				placeQueue.offer( id ); 
				placeAddedCount ++;
			}
		}
		
		statusLog("Get users id list: " + ", count " + personAddedCount + "/"+ personsIdList.size() );
		statusLog("Get places id list: " + ", count " + placeAddedCount + "/"+ placesIdList.size() );

		// Run person processor thread
		personProcessor = new PersonProcessor(config, personQueue, placeQueue );
		new Thread( personProcessor ).start();
		
		// Run place processor thread
		placeProcessor = new PlaceProcessor(config, personQueue, placeQueue );
		new Thread( placeProcessor ).start();
		
		return true;
	}
	
	/**
	 * get checkins from lat lng, results will be added to queues 
	 * @param lat
	 * @param lng
	 */
	private void getCheckinsFromLatLng(double lat, double lng, List<String> personsIdList, List<String> placesIdList){
		
		// get checkins from lat/lng		
		String getCheckinsFromLatLngUrl = OBJECTS_URL + "?filters=checkins&latitude=" + lat + "&longitude=" + lng + "&radius=" + SEARCH_RADIUS + "&limit=" + PAGE_SIZE;
		log("Req : Get Checkins, " + getCheckinsFromLatLngUrl);
		
		String result;	
		try {
			result = HttpUtil.requestByGet( getCheckinsFromLatLngUrl , HTTP_TIMEOUT );
			JSONArray checkinsJson = new JSONArray( result );
			int checkinInsertCount = 0;
			for(int i=0; i < checkinsJson.length(); i ++ ){
				JSONObject obj = checkinsJson.getJSONObject(i);
				UserCheckin item = UserCheckin.fromJSON( obj );
				String oldCheckinId = BrightkiteDBManager.checkCheckin(item.getId(), item.getCreated_at());
				
				// Save checkin to CheckinDB
				if( oldCheckinId == null ){
					boolean ret = BrightkiteDBManager.addUserCheckin( item );
					log("Insert Checkin: " + item.getId() + ", " + ret);
					if( ret ){
						checkinInsertCount ++;
					}
				}else{
					log("Checkin Exists: " + oldCheckinId);
				}
				
				// put person ID 
				personsIdList.add( item.getUser_id() ); 
				// put place ID 
				placesIdList.add( item.getSpot_id() );
			}
			
			logger.info("Get Checkins:" + checkinInsertCount + "/" + checkinsJson.length() );
			log( this.getName() + " : Get Checkins, " + checkinInsertCount + "/" + checkinsJson.length() );
			
			sleep();
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Error in get checkins from lat/lng", e);
			statusLog( this.getName() + " : Get checkins from lat/lng error." );
		}	
	}
	
	public void stop(){
		statusLog( this.getName() + ".stop, " );
		
		if( personProcessor != null ){
			personProcessor.stop();
		}
		if( placeProcessor != null ){
			placeProcessor.stop();
		}
		
		super.stop();
	}

	private static void sleep(){
		try
		{
			Thread.sleep(POLL_INTERVAL);
		}
		catch (InterruptedException e){}	
	}
	
	public BlockingQueue<String> getPersonQueue() {
		return personQueue;
	}
	
	public BlockingQueue<String> getPlaceQueue() {
		return placeQueue;
	}

}
