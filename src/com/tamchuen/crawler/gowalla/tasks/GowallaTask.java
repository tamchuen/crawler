package com.tamchuen.crawler.gowalla.tasks;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.tamchuen.crawler.Config;
import com.tamchuen.crawler.gowalla.db.GowallaDBManager;
import com.tamchuen.crawler.tasks.AbstractTask;
import com.tamchuen.jutil.string.StringUtil;
import com.tamchuen.jutil.util.FileUtil;
import com.tamchuen.jutil.util.Pair;
/**
 * The abstract task for crawling gowalla.com using gowalla API
 * As of 2011 10/29, the id range is as follows:
 * spot ID from 9,030 to 1,601,000,
 * user ID from 1 to 550,100,
 * checkin ID from 101 to 44,651,000
 * @author Dequan
 *
 */
public abstract class GowallaTask extends AbstractTask{
	/**
	 * these API keys can be selected randomly to access the Gowalla HTTP API
	 */
	public static final String[] GOWALLA_API_KEYS = {"eb904ea48bf04341bbfcbcf677a02bc6","942e74ee6b3e4260a78ff97efc863d5e","35c0cb503dfa40efaae60c031f7b2c08","4a5d9bf7cc04475682582a4888d1e296","e24e13f60ffa48b3973e51150822b367","07aff5df50dc4329b9f293f67d466e12","5ad9974b76444dbc96d2d0ddac30c7a5","077dd177f8f949c199985c3a0ebc143f"};
	public static final String GOWALLA_USERS_URL = "http://api.gowalla.com/users/";
	public static final String GOWALLA_SPOTS_URL = "http://api.gowalla.com/spots/";
	public static final String GOWALLA_CHECKINS_URL = "http://api.gowalla.com/checkins/";

	public static final long MIN_USER_ID = 1;
	public static final long MAX_USER_ID = 550100;
	public static final long MIN_SPOT_ID = 9030;
	public static final long MAX_SPOT_ID = 1601000L;
	public static final long MIN_CHECKIN_ID = 101;
	public static final long MAX_CHECKIN_ID = 44651000L;


	private List<Map<String,String>> headersList = new ArrayList<Map<String,String>>();

	protected String currentId;


	private Random random;
	public GowallaTask() {
		random = new Random( GOWALLA_API_KEYS.length -1 );
		for(String apiKey : GOWALLA_API_KEYS){
			Map<String,String> header = new HashMap<String,String>();
			header.put("Accept", "application/json");
			header.put("X-Gowalla-API-Key", apiKey);
			headersList.add( header );
		}
	}
	/**
	 * the HTTP Header for accessing gowalla API
	 * @return
	 */
	public Map<String,String> getHeader(){
		return headersList.get(  random.nextInt(GOWALLA_API_KEYS.length) );
	}

	/**
	 * init the queue for task IDs (user/spot IDs)
	 */
	public void initialize(Config config){
		super.initialize(config);

		boolean isSpotId = getName().startsWith("Spot");

		long start = System.currentTimeMillis();
		long totalCount = 0;
		long existsCount = 0;
		long addedCount = 0;

		List<String> idList = new ArrayList<String>(5000);

		// get raw id list
		if(config.isRangeFromFile() ){
			// it's given from a file, get ids from file and add to the queue
			idList = FileUtil.readFile2List(config.getRangeFilePath(), "UTF-8");
			if( idList == null ){
				statusLog("Get range failed from file : " + config.getRangeFilePath()  );
				return;
			}

			statusLog("Get Ids from file : " + config.getRangeFilePath()  );
		}else{
			long minId = 0;
			long maxId = 0;
			// it's given from a min/max range
			// use spot id range
			Pair<Long,Long> idRanges = isSpotId ? config.getSpotIdRange() : config.getUserIdRange();
			minId = idRanges.first;
			maxId = idRanges.second;

			for(long i=minId; i <= maxId; i ++ ){
				idList.add( String.valueOf(i) );
			}

			statusLog("Get Ids from range : " + minId + " - "  + maxId);
		}

		// check if need to check duplicate
		if( config.isCheckDuplicateId() ){
			// check DB first
			for(String id : idList ){
				long idToCheck = StringUtil.convertLong(id, -1);
				if( idToCheck < 0 ){
					continue;
				}

				long oldId = isSpotId ? GowallaDBManager.checkSpot(idToCheck ) :GowallaDBManager.checkUser( idToCheck );
				if( oldId > 0 ){
					existsCount ++;
				}else{
					taskIdQueue.offer( id );
					addedCount ++;
				}  
				totalCount ++;
			}
		}else{
			// add to task Queue directly
			for(String id : idList ){
				long idToCheck = StringUtil.convertLong(id, -1);
				if( idToCheck < 0 ){
					continue;
				}

				taskIdQueue.offer( id );
				addedCount ++;
				totalCount ++;
			}
		}
		long end = System.currentTimeMillis(); 
		long elapsedTime = end - start;
		statusLog("Get " + totalCount + " out of " + idList.size() + (isSpotId?" spot":" user") + " Ids, " + addedCount + " added, "   + existsCount + " ignored, using " + elapsedTime + " ms." );
	}

	public void stop(){
		statusLog( this.getName() + ".stop, current ID:" + currentId );
		super.stop();
	}
	
	public void sleepFor(long interval){
		try
		{
			Thread.sleep(interval);
		}
		catch (InterruptedException e){}
	}

	public static void main(String[] args){
		Random random = new Random(  );
		// test random
		for(int i=0; i < 10; i ++ ){
			System.out.println("INT:" + random.nextInt(GOWALLA_API_KEYS.length));
		}
	}

}
