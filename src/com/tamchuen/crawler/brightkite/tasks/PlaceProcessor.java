package com.tamchuen.crawler.brightkite.tasks;

import java.net.SocketTimeoutException;
import java.util.concurrent.BlockingQueue;

import org.json.JSONArray;
import org.json.JSONObject;

import com.tamchuen.crawler.Config;
import com.tamchuen.crawler.brightkite.db.BrightkiteDBManager;
import com.tamchuen.crawler.brightkite.domain.Spot;
import com.tamchuen.crawler.brightkite.domain.UserCheckin;
import com.tamchuen.crawler.db.DuplicateException;
import com.tamchuen.crawler.tasks.AbstractTask;
import com.tamchuen.crawler.tasks.AbstractTask.TaskCode;
import com.tamchuen.crawler.util.HttpUtil;
import com.tamchuen.jutil.j4log.Logger;

/**
 * Processor thread for processing each spot/place from the place queue
 * @author  Dequan
 * Project: Crawler
 * Date:    Dec 1, 2011
 * 
 */
public class PlaceProcessor implements Runnable{
	public static final String NAME = PlaceProcessor.class.getSimpleName();
	private static final Logger logger = Logger.getLogger( NAME );
	
	private BlockingQueue<String> personQueue;
	private BlockingQueue<String> placeQueue;
	private Config config;
	private boolean running;
	
	
	public PlaceProcessor(Config config, BlockingQueue<String> personQueue, BlockingQueue<String> placeQueue){
		this.personQueue = personQueue;
		this.placeQueue = placeQueue;
		this.config = config;
		running = true;
	}
	
	@Override
	public void run() {
		while( running ){
//		     take one place ID from PlaceQueue,
			String spotId = null;
			try {
				spotId = placeQueue.take();
			} catch (InterruptedException e) {
				e.printStackTrace();
				logger.error( "Error in take from place queue ", e);
			}
			
			if( spotId == null ){
				logger.error( "Error: spot ID is null");
				stop();
			}
			
			String httpResult = null;
			boolean dbResult = true;
			JSONObject json = null;
			Spot spot = null;
			int pageSize = BrightkiteTask.PAGE_SIZE;
			TaskCode taskCode = TaskCode.UNKNOWN;
			
			String oldSpotId = BrightkiteDBManager.checkSpot( spotId);
			if( oldSpotId != null ){
				System.out.println("Spot Exists: " + oldSpotId);
				continue;
			}
			
//		    Places API -> get place detail
			String getSpot = BrightkiteTask.SPOTS_URL + spotId + ".json";

			logger.info( NAME + ": Req Get Spot, " + getSpot);
			config.getLogPanel().log( NAME + ": Req Get Spot, " + getSpot );

			try {
				httpResult = HttpUtil.requestByGet( getSpot, BrightkiteTask.HTTP_TIMEOUT );
			} catch (Exception e) {
				e.printStackTrace();
				logger.error( "HTTP Error for spot " + spotId, e);
				taskCode = TaskCode.HTTP_ERROR;
			}

			if( httpResult != null ){
				try {
					json = new JSONObject( httpResult );
					spot = Spot.fromJSON( spotId, json );
					//					     Save place into PlaceDB
					dbResult = BrightkiteDBManager.addSpot( spot );
					taskCode = dbResult ? TaskCode.SUCCESS : TaskCode.DB_ERROR;
				} 
				catch (DuplicateException e){
					taskCode = TaskCode.DB_DUPLICATE;
				}
				catch (Exception e) {
					e.printStackTrace();
					logger.error( "JSON exception for spot " + spotId, e);
				}	
			}
				
			System.out.println(NAME + ": Insert Spot " + spotId + "," + AbstractTask.getMsgFromCode(taskCode));
			config.getLogPanel().log(NAME + ": Insert Spot " + spotId + "," + AbstractTask.getMsgFromCode(taskCode));

			sleep();

			if( spot == null ){
				continue;
			}
				
			// controls whether need to get further checkin list for this place
			boolean isInRange = true;
			String city = spot.getCity();
			if( city==null || city.equals("null") || city.trim().length() < 1 ){
				isInRange = false;
			}
			else if( config.isFilterCities()  ){
				isInRange = BrightkiteTask.TOP_CITIES.contains( city );
			}
			 
			if( !isInRange ){
				logger.info( NAME + ": Spot not in range, " + spot.getId() + "," + spot.getCity() );
	    		config.getLogPanel().log(NAME +": Spot not in range, " + spot.getId() + "," + spot.getCity() );
				continue;
			}
			
//		     Objects API -> get Checkins according to place ID
			int pageOfCheckinsFromSpot = 0;
			boolean hasNextPage = true;
	    	int checkinInsertCount = 0;
	    	int checkinTotalCount = 0 ;
	    	int retryCount = 0;
	    	boolean isRetry = false;
	    	
	    	while( hasNextPage ){
	    		String getCheckinsFromSpotUrl = BrightkiteTask.OBJECTS_URL + "?filters=checkins&place_id=" + spotId + "&offset=" + (pageOfCheckinsFromSpot*pageSize) + "&limit=" + pageSize;
	    		
	    		System.out.println(NAME + " :Req " + (isRetry ?"retry":"") +": Get Checkins, " + getCheckinsFromSpotUrl);
	    		logger.info( NAME + " :Req " + (isRetry ?"retry":"") +": Get Checkins, " + getCheckinsFromSpotUrl );
	    		config.getLogPanel().log(NAME + " :Req " + (isRetry ?"retry":"") +": Get Checkins, " + getCheckinsFromSpotUrl);
	    		
	    		isRetry = false;
	    		try {
	    			httpResult = HttpUtil.requestByGet( getCheckinsFromSpotUrl , BrightkiteTask.HTTP_TIMEOUT );

	    			JSONArray checkinsJson = new JSONArray( httpResult );
	    			for(int i=0; i < checkinsJson.length(); i ++ ){
	    				JSONObject obj = checkinsJson.getJSONObject(i);
	    				UserCheckin item = UserCheckin.fromJSON( obj );
	    				String oldCheckinId = BrightkiteDBManager.checkCheckin(item.getId(), item.getCreated_at());
	    				if( oldCheckinId == null ){
//	    				     Save checkin to CheckinDB
	    					dbResult = BrightkiteDBManager.addUserCheckin( item );
	    					//System.out.println("Insert Checkin: " + ret);
	    					if( dbResult ){
	    						checkinInsertCount ++;
	    					}
	    				}else{
	    					//System.out.println("Checkin Exists: " + oldCheckinId);
	    				}
	    				
//	    			     put each user ID into PersonQueue
	    				if( ! personQueue.contains( item.getUser_id() )){
	    					config.getLogPanel().log(NAME + ": Add user to queue " + item.getUser_id() );
	    					personQueue.add(item.getUser_id());
	    				}
	    				
	    				checkinTotalCount ++;
	    			}

	    			if(  checkinsJson.length() < pageSize ){
	    				hasNextPage = false;
	    			}
	    		} 
	    		catch( SocketTimeoutException e){
	    			// check read time out exception: Read timed out
    				retryCount ++;
    				if( retryCount >= BrightkiteTask.MAX_RETRY_COUNT ){
    					hasNextPage = false;
    				}else{
    					hasNextPage = true;
    					isRetry = true;
    				}
	    		}
	    		catch (Exception e) {
	    			e.printStackTrace();
	    			logger.error( "Error in get checkins from spot " + spotId, e);
	    			// check HTTP 501 :Sorry, but the page number you have requested is too high.
	    			// or HTTP 503 service unavailable
	    			if( e.getMessage() !=null && e.getMessage().indexOf(": 50") >0){
	    				hasNextPage = false;
	    			}
	    		}
	    		
	    		if( !isRetry ){
	    			pageOfCheckinsFromSpot ++;
	    		}
	    		
	    		// maximum pages 
	    		if( pageOfCheckinsFromSpot > BrightkiteTask.MAX_PAGE_NUMBER){
	    			hasNextPage = false;
	    		}
	    		
	    		config.getLogPanel().log(NAME + ": Get Checkins:" + checkinInsertCount + "/" + checkinTotalCount + " for spot " + spotId);
	    		// System.out.println("Get Checkins:" + checkinInsertCount + "/" + checkinTotalCount );
	    		sleep();
	    	}// end while next page
		    
			
		}// end while true
	}
	
	private static void sleep(){
		try
		{
			Thread.sleep(BrightkiteTask.POLL_INTERVAL);
		}
		catch (InterruptedException e){}	
	}
	
	public void stop(){
		running = false;
		config.getStatusPanel().log(NAME + ": Stopped.");
	}
}
