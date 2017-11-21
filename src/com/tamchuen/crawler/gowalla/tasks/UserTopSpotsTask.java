package com.tamchuen.crawler.gowalla.tasks;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.json.JSONArray;
import org.json.JSONObject;

import com.tamchuen.crawler.Config;
import com.tamchuen.crawler.db.DuplicateException;
import com.tamchuen.crawler.gowalla.db.GowallaDBManager;
import com.tamchuen.crawler.gowalla.domain.UserSpot;
import com.tamchuen.crawler.util.HttpUtil;
import com.tamchuen.jutil.j4log.Logger;
import com.tamchuen.jutil.util.Pair;

/**
 * Get User Spot relationship information from user's top spots.
 * @author Dequan
 *
 */
public class UserTopSpotsTask extends GowallaTask {
	public static final String NAME = "UserTopSpotsTask";
	private static final Logger logger = Logger.getLogger(NAME);
	private static int POLL_INTERVAL = 5000;

	public UserTopSpotsTask() {
		this.setName( NAME );
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
				while ( (id = taskIdQueue.poll()) != null && TaskStatus.STARTED.equals( UserTopSpotsTask.this.getStatus()) )
				{
					long start = System.currentTimeMillis();
					long userId = Long.valueOf(id);
					currentId = id;

					String apiUrl = GOWALLA_USERS_URL + userId + "/top_spots";
					String httpResult = null;

					TaskCode taskCode = TaskCode.UNKNOWN;
					boolean dbResult = false;
					List<UserSpot> dataList = new ArrayList<UserSpot>();

					// Get HTTP content
					try
					{
						httpResult = HttpUtil.requestByGet( apiUrl, null, getHeader() );
					}
					catch (FileNotFoundException e)
					{
						// the user doesn't exist
						taskCode = TaskCode.NOT_FOUND;
					}
					catch( Exception e){
						// possibly 500 internel ERROR, should decide further reason for this
						// if the user doesn't exist, then the apiUrl will return 500
						logger.error(NAME + ".background thread HTTP exception", e);
						taskCode = TaskCode.HTTP_ERROR;
					}

					// Process HTTP content
					if( httpResult != null ){
						try
						{
							JSONObject json = new JSONObject( httpResult );
							JSONArray items = json.getJSONArray("top_spots" );
							for(int i=0; i < items.length(); i ++ ){
								JSONObject obj = items.getJSONObject(i);
								UserSpot item = UserSpot.fromUserTopSpots(userId,  obj);
								dataList.add( item );
							}
						}
						catch (Exception e)
						{
							logger.error(NAME + ".background thread JSON exception", e);
							taskCode = TaskCode.JSON_ERROR;
						}
					}
					
					int insertCount = 0;
					// add to DB
					if( dataList.size() > 0 ){
						try
						{
							for(int i=0; i < dataList.size(); i ++ ){
								UserSpot newUserSpot = dataList.get(i);
								// check if exists before
								UserSpot oldSpot = GowallaDBManager.getUserSpot(newUserSpot.getUser_id(), newUserSpot.getSpot_id());
								if( oldSpot == null  ){
									// System.out.println("INSERT:" + newUserSpot);
									dbResult = GowallaDBManager.addUserSpot( newUserSpot );
									insertCount ++;
								}else{
									newUserSpot.setId( oldSpot.getId() );
									// only need to update if the checkins count is greater
									if( oldSpot.getUser_checkins_count() < newUserSpot.getUser_checkins_count() ){
										//System.out.println("UPDATE " + newUserSpot);
										dbResult = GowallaDBManager.updateUserSpot( newUserSpot );
									}else{
										dbResult = true;
										//System.out.println("No need UPDATE");
									} 
								}
								// check result
								taskCode = dbResult ? TaskCode.SUCCESS : TaskCode.DB_ERROR;
							}
						}
						catch (DuplicateException e)
						{
							taskCode = TaskCode.DB_DUPLICATE;
						}
					}

					long end = System.currentTimeMillis(); 
					long elapsedTime = end - start;

					// if it's UNKOWN, that means that user has no top spots
					log(NAME + " " + userId + " : " +  getMsgFromCode(taskCode) + ", spots count:" + insertCount + "/"+ dataList.size() + ", elapsed " + elapsedTime );
					logger.info(NAME + " " + userId + " : " +  getMsgFromCode(taskCode)+ ", spots count:" + insertCount + "/" + dataList.size() + ", elapsed " + elapsedTime);

					// try to sleep for a while to avoid rate limit
					if( elapsedTime < POLL_INTERVAL){
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
