package com.tamchuen.crawler.brightkite.tasks;

import java.net.SocketTimeoutException;
import java.util.concurrent.BlockingQueue;

import org.json.JSONArray;
import org.json.JSONObject;

import com.tamchuen.crawler.Config;
import com.tamchuen.crawler.brightkite.db.BrightkiteDBManager;
import com.tamchuen.crawler.brightkite.domain.Spot;
import com.tamchuen.crawler.brightkite.domain.User;
import com.tamchuen.crawler.brightkite.domain.UserCheckin;
import com.tamchuen.crawler.brightkite.domain.UserFriend;
import com.tamchuen.crawler.db.DuplicateException;
import com.tamchuen.crawler.tasks.AbstractTask;
import com.tamchuen.crawler.tasks.AbstractTask.TaskCode;
import com.tamchuen.crawler.util.HttpUtil;
import com.tamchuen.jutil.j4log.Logger;
import com.tamchuen.jutil.util.MappedQueue;
import com.tamchuen.jutil.util.Pair;

/**
 * Processor thread for processing each user from the user queue
 * 
 * @author  Dequan
 * Project: Crawler
 * Date:    Dec 1, 2011
 * 
 */
public class PersonProcessor implements Runnable{
	public static final String NAME = PersonProcessor.class.getSimpleName();
	private static final Logger logger = Logger.getLogger( NAME );
	
	private BlockingQueue<String> personQueue;
	private BlockingQueue<String> placeQueue;
	private Config config;
	private boolean running;
	
	public PersonProcessor(Config config, BlockingQueue<String> personQueue, BlockingQueue<String> placeQueue){
		this.personQueue = personQueue;
		this.placeQueue = placeQueue;
		this.config = config;
		running = true;
	}
	
	@Override
	public void run() {
		while( running ){
//		     take one person ID from PersonQueue,
			String userId = null;
			try {
				userId = personQueue.take();
			} catch (InterruptedException e) {
				e.printStackTrace();
				logger.error( "Error in take from person queue ", e);
			}
			
			if( userId == null ){
				logger.error( "Error: userId ID is null");
				stop();
			}
			
			String httpResult = null;
			boolean dbResult = true;
			JSONObject json = null;
			User user = null;
			int pageSize = BrightkiteTask.PAGE_SIZE;
			TaskCode taskCode = TaskCode.UNKNOWN;
			
			String oldUserId = BrightkiteDBManager.checkUser( userId);
			if( oldUserId != null ){
				System.out.println("User Exists: " + oldUserId);
				continue;
			}	
			
//		    People API -> get user detail, checkins count, friends count
			String getUser = BrightkiteTask.USERS_URL + userId + ".json";

			logger.info( NAME + ": Req Get User, " + getUser);
			config.getLogPanel().log( NAME + ": Req Get User, " + getUser );

			try {
				httpResult = HttpUtil.requestByGet( getUser, BrightkiteTask.HTTP_TIMEOUT );
			} catch (Exception e) {
				e.printStackTrace();
				logger.error( "HTTP Error for user " + userId, e);
				taskCode = TaskCode.HTTP_ERROR;
			}

			if( httpResult!= null ){
				try {
					json = new JSONObject( httpResult );
					user = User.fromJSON( userId, json );
					//				     Save person to PersonDB
					dbResult = BrightkiteDBManager.addUser( user );
					// check result
					taskCode = dbResult ? TaskCode.SUCCESS : TaskCode.DB_ERROR;
					// TODO: save Badges 
				} 
				catch (DuplicateException e)
				{
					taskCode = TaskCode.DB_DUPLICATE;
				}
				catch (Exception e) {
					e.printStackTrace();
					logger.error( "JSON exception for user " + userId, e);
				}	
			}

			System.out.println(NAME + ": Insert User " + userId + "," + AbstractTask.getMsgFromCode(taskCode));
			config.getLogPanel().log(NAME + ": Insert User " + userId + "," + AbstractTask.getMsgFromCode(taskCode));

			sleep();

			if( user == null ){
				continue;
			}
			
  
//		     Objects API-> get Checkins according to person ID, checkins count ( Pagination )
		    int userCheckinsCount = user.getCheckins_count();
		    if( userCheckinsCount > 0 ){
		    	int noOfPages = userCheckinsCount/pageSize + 1;
		    	int checkinInsertCount = 0;
		    	int checkinTotalCount = 0 ;
		    	int retryCount = 0;
		    	boolean isRetry = false;
		    	
		    	System.out.println("Get User Checkins: Total Count " + userCheckinsCount + ", pageSize:" + pageSize + ", pages:" + noOfPages );
		    	config.getLogPanel().log("Get User Checkins: Total Count " + userCheckinsCount + ", pageSize:" + pageSize + ", pages:" + noOfPages );
		    	
		    	for(int p=0; p < noOfPages; p++ ){
		    		String getCheckinsFromUserUrl = BrightkiteTask.OBJECTS_URL + "?filters=checkins&person_id=" + userId + "&offset=" + (p*pageSize) + "&limit=" + pageSize;

		    		logger.info(NAME + ": Req " + (isRetry ?"retry":"") +": Get Checkins " + (p+1) + ", " + getCheckinsFromUserUrl);
		    		config.getLogPanel().log(NAME + ": Req " + (isRetry ?"retry":"") +": Get Checkins " + (p+1) + ", " + getCheckinsFromUserUrl);
		    		
		    		isRetry = false;
		    		try {
		    			httpResult = HttpUtil.requestByGet( getCheckinsFromUserUrl , BrightkiteTask.HTTP_TIMEOUT );
		    			JSONArray checkinsJson = new JSONArray( httpResult );
		    			
		    			for(int i=0; i < checkinsJson.length(); i ++ ){
		    				JSONObject obj = checkinsJson.getJSONObject(i);
		    				UserCheckin item = UserCheckin.fromJSON( obj );
		    				String oldCheckinId = BrightkiteDBManager.checkCheckin(item.getId(), item.getCreated_at());
		    				if( oldCheckinId == null ){
//		    				     Save checkin to CheckinDB
		    					dbResult = BrightkiteDBManager.addUserCheckin( item );
		    					//System.out.println("Insert Checkin: " + ret);
		    					if( dbResult ){
		    						checkinInsertCount ++;
		    					}
		    				}else{
		    					//System.out.println("Checkin Exists: " + oldCheckinId);
		    				}
		    				
//		    			     put each place ID into PlaceQueue
		    				if(! placeQueue.contains(item.getSpot_id())){
		    					config.getLogPanel().log(NAME + ": Add place to queue " + item.getSpot_id() );
		    					placeQueue.add(item.getSpot_id());
		    				}
		    				
		    				if( placeQueue.size() > 0 && placeQueue.size() % 100 ==0){
		    					config.getLogPanel().log(NAME + ":place queue size " + placeQueue.size() );
		    				}
		    				checkinTotalCount ++;
		    			}

		    		} 
		    		catch( SocketTimeoutException e){
		    			// check read time out exception: Read timed out
	    				retryCount ++;
	    				if( retryCount < BrightkiteTask.MAX_RETRY_COUNT ){
	    					isRetry = true;
	    					p--;
	    				}
		    		}
		    		catch (Exception e) {
		    			e.printStackTrace();
		    			logger.error( "Error in get checkins from user " + userId, e);
		    		}
		    		
		    		config.getLogPanel().log(NAME + ": Get Checkins:" + checkinInsertCount + "/" + checkinTotalCount + " for user " + user.getId());
		    		sleep();
		    	}
		    }	    
		    
		    
//		     People API-> get friends according to person ID, friends count ( Pagination )
			int friendsCount = 	user.getFriends_count();
			if( friendsCount > 0 ){
				int friendsInsertCount = 0;
				int friendsTotalCount = 0;
				int noOfPages = friendsCount/pageSize + 1;
				int retryCount = 0;
		    	boolean isRetry = false;
		    	
				System.out.println( NAME + ": Get User Friends: Total Count " + friendsCount + ", pageSize:" + pageSize + ", pages:" + noOfPages );
		    	config.getLogPanel().log(NAME + ":Get User Friends: Total Count " + friendsCount + ", pageSize:" + pageSize + ", pages:" + noOfPages );

				for(int p=0; p < noOfPages; p++ ){
					String getUserFriends = BrightkiteTask.USERS_URL + userId + "/friends.json?offset=" + (p*pageSize) + "&limit=" + pageSize;
					
					System.out.println(NAME + " :Req " + (isRetry ?"retry":"") +": Get User Friends " + (p+1) + ", " + getUserFriends);
					logger.info(NAME + " :Req " + (isRetry ?"retry":"") +": Get User Friends " + (p+1) + ", " + getUserFriends);
					config.getLogPanel().log(NAME + " :Req " + (isRetry ?"retry":"") +": Get User Friends " + (p+1) + ", " + getUserFriends);
					
					isRetry = false;
					try {
						httpResult = HttpUtil.requestByGet( getUserFriends, BrightkiteTask.HTTP_TIMEOUT );

						JSONArray friendsJson = new JSONArray( httpResult );
						for(int i=0; i < friendsJson.length(); i ++ ){
							JSONObject obj = friendsJson.getJSONObject(i);
							// Save friends to UserFriendsDB
							UserFriend item = UserFriend.fromJSON(userId,  obj);
							
							// check if exists first
							String oldUserFriendId = BrightkiteDBManager.checkUserFriend(userId, item.getUser2() );
		    				if( oldUserFriendId == null ){
//		    				     Save checkin to CheckinDB
		    					dbResult = BrightkiteDBManager.addUserFriend( item );
		    					//System.out.println("Insert UserFriend: " + ret);
		    					if( dbResult ){
		    						friendsInsertCount ++;
		    					}
		    				}else{
		    					//System.out.println("UserFriend Exists: " + oldUserFriendId);
		    				}
							
//						     put each person ID of friend into PersonQueue 
							/*
		    				if( ! personQueue.contains( item.getUser2() )){
								config.getLogPanel().log(NAME + ": Add person to queue " + item.getUser2() );
								personQueue.add( item.getUser2() );
		    				}*/
							
							if(personQueue.size() >0 && personQueue.size() % 100 ==0){
		    					config.getLogPanel().log(NAME + ":person queue size " + personQueue.size() );
		    				}
							
							friendsTotalCount ++;
						}

					} 
					catch( SocketTimeoutException e){
		    			// check read time out exception: Read timed out
	    				retryCount ++;
	    				if( retryCount < BrightkiteTask.MAX_RETRY_COUNT ){
	    					isRetry = true;
	    					p--;
	    				}
		    		}
					catch (Exception e) {
						e.printStackTrace();
						logger.error( "Error in get friends from user " + userId, e);
					}
					
					config.getLogPanel().log(NAME + ": Get User Friends:" + friendsInsertCount + "/" + friendsTotalCount + " for user " + user.getId() );
					sleep();
				}
			}// end if

			
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
