package com.tamchuen.crawler.gowalla.tasks;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

import com.tamchuen.crawler.Config;
import com.tamchuen.crawler.db.DuplicateException;
import com.tamchuen.crawler.gowalla.db.GowallaDBManager;
import com.tamchuen.crawler.gowalla.domain.UserCheckin;
import com.tamchuen.crawler.gowalla.domain.UserSpot;
import com.tamchuen.crawler.tasks.AbstractTask.TaskCode;
import com.tamchuen.crawler.util.HttpUtil;
import com.tamchuen.jutil.j4log.Logger;
import com.tamchuen.jutil.util.Pair;

/**
 * Get Checkin detail information according to Spot ID,
 * Currently the API only returns the recent 20 checkins 
 * @author Dequan
 */
public class SpotEventsTask extends GowallaTask {
	public static final String NAME = "SpotEventsTask";
	private static final Logger logger = Logger.getLogger( NAME );
	private static int POLL_INTERVAL = 5000;

	boolean needUpdateUserSpot = true;

	public SpotEventsTask() {
		this.setName(NAME);
	}

	public boolean start(){
		// set status as started
		super.start();

		// calculate where to start
		final Config config = this.getConfig();

		new Thread(NAME + ".background")
		{
			@Override
			public void run()
			{
				logger.info(NAME + ".start|start background thread:" + Thread.currentThread());
				String id = null;

				// loop
				while ( (id = taskIdQueue.poll()) != null && TaskStatus.STARTED.equals( SpotEventsTask.this.getStatus()) )
				{
					long start = System.currentTimeMillis();
					long spotId = Long.valueOf(id);
					currentId = id;

					String apiUrl = GOWALLA_SPOTS_URL + spotId + "/events";
					String httpResult = null;

					TaskCode taskCode = TaskCode.UNKNOWN;
					// in case the checkins for the spotId has been in DB, although there is no insert
					// the result should be success
					boolean dbResult = true;
					List<UserCheckin> dataList = new ArrayList<UserCheckin>();

					// Get HTTP content
					try
					{
						httpResult = HttpUtil.requestByGet( apiUrl, null, getHeader() );
					}
					catch (FileNotFoundException e)
					{
						// the Spot doesn't exist
						taskCode = TaskCode.NOT_FOUND;
					}
					catch( Exception e){
						logger.error(NAME + ".background thread HTTP exception", e);
						taskCode = TaskCode.HTTP_ERROR;
					}

					// Process HTTP content
					if( httpResult != null ){
						try
						{
							JSONObject json = new JSONObject( httpResult );
							JSONArray items = json.getJSONArray("activity" );
							for(int i=0; i < items.length(); i ++ ){
								JSONObject obj = items.getJSONObject(i);
								UserCheckin item = UserCheckin.fromSpotEvents(spotId,  obj);
								if( item != null ){
									dataList.add( item );
								}
							}
						}
						catch (Exception e)
						{
							logger.error(NAME + ".background thread JSON exception", e);
							taskCode = TaskCode.JSON_ERROR;
						}
					}

					int insertCount1 = 0;
					int insertCount2 = 0;
					// add to DB
					if( dataList.size() > 0 ){
						try
						{
							for(int i=0; i < dataList.size(); i ++ ){
								UserCheckin checkin = dataList.get(i);
								
								// check for checkin in DB
								long oldId = GowallaDBManager.checkCheckin( checkin.getId() );
								if( oldId == -1){
									dbResult = GowallaDBManager.addUserCheckin( checkin );
									if( dbResult ){
										insertCount1 ++;
									}	
								} 
								
								// check for userSpot in DB
								oldId = GowallaDBManager.checkUserSpot( checkin.getUser_id(), checkin.getSpot_id() );
								
								// check if exists before
								if( oldId == -1  ){
									UserSpot userSpot = new UserSpot();
									userSpot.setUser_id(checkin.getUser_id());
									userSpot.setSpot_id(checkin.getSpot_id());
									userSpot.setUser_checkins_count(1);
									dbResult = GowallaDBManager.addUserSpot( userSpot );
									insertCount2 ++;
								}
							}
							
							taskCode = dbResult ? TaskCode.SUCCESS : TaskCode.DB_ERROR;
						}
						catch (DuplicateException e)
						{
							taskCode = TaskCode.DB_DUPLICATE;
						}
					} 
					
					long end = System.currentTimeMillis(); 
					long elapsedTime = end - start;
					log(NAME + " " + spotId + " : " +  getMsgFromCode(taskCode) + ", checkins:" + insertCount1 + "/" + insertCount2 + "/"+ dataList.size() + ", elapsed " + elapsedTime);
					logger.info(NAME + " " + spotId + " : " + getMsgFromCode(taskCode) + ", checkins:" + insertCount1 + "/"+ insertCount2 + "/" + dataList.size() + ", elapsed " + elapsedTime);

					// try to sleep for a while to avoid rate limit
					if( elapsedTime < POLL_INTERVAL ){
						sleepFor(POLL_INTERVAL);
					}
				}

				logger.info(NAME + ".background thread terminated:" + Thread.currentThread() + ", current ID:" + currentId  );
				config.getStatusPanel().log(NAME + " finished normally with current ID:" + currentId);
			}
		}.start();

		return true;
	}

}
