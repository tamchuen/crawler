package com.tamchuen.crawler.gowalla.tasks;

import java.io.FileNotFoundException;

import org.json.JSONArray;
import org.json.JSONObject;

import com.tamchuen.crawler.Config;
import com.tamchuen.crawler.db.DuplicateException;
import com.tamchuen.crawler.gowalla.db.GowallaDBManager;
import com.tamchuen.crawler.gowalla.domain.Spot;
import com.tamchuen.crawler.gowalla.domain.UserSpot;
import com.tamchuen.crawler.tasks.AbstractTask.TaskCode;
import com.tamchuen.crawler.util.HttpUtil;
import com.tamchuen.jutil.j4log.Logger;
import com.tamchuen.jutil.util.Pair;

/**
 * Get Spot detail information
 * @author Dequan
 *
 */
public class SpotTask extends GowallaTask {
	public static final String NAME = "SpotTask";
	private static final Logger logger = Logger.getLogger( NAME );
	private static int POLL_INTERVAL = 5000;
	// need to update userSpot according to top10

	boolean needUpdateUserSpot = true;

	public SpotTask() {
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
				while ( (id = taskIdQueue.poll()) != null && TaskStatus.STARTED.equals( SpotTask.this.getStatus()) )
				{
					long start = System.currentTimeMillis();
					long spotId = Long.valueOf(id);
					currentId = id;

					String apiUrl = GOWALLA_SPOTS_URL + spotId;
					String httpResult = null;

					TaskCode taskCode = TaskCode.UNKNOWN;
					boolean dbResult = false;
					JSONObject json = null;

					// check DB first
					long oldId = GowallaDBManager.checkSpot(spotId);
					if( oldId > 0 ){
						// record exists in DB, ignore this if it's not in update mode
						if( ! config.isUpdateMode() ){
							taskCode = TaskCode.EXISTS;
							log(NAME + " " + spotId + " : " +  getMsgFromCode(taskCode) );
							continue;
						}
					} 

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
							json = new JSONObject( httpResult );
							Spot item = Spot.fromJSON( spotId, json );
							// update the record if it exists before
							dbResult = oldId > 0 ? GowallaDBManager.updateSpot( item ) : GowallaDBManager.addSpot( item );
							// check result
							taskCode = dbResult ? TaskCode.SUCCESS : TaskCode.DB_ERROR;
						}
						catch (DuplicateException e)
						{
							taskCode = TaskCode.DB_DUPLICATE;
						}
						catch (Exception e)
						{
							logger.error(NAME + ".background thread JSON exception", e);
							taskCode = TaskCode.JSON_ERROR;
						}
					}   


					// need to update the user_checkins_count according to top10
					int updateCount = 0;
					int insertCount = 0;
					if( needUpdateUserSpot ){
						boolean topTenRet = false;
						JSONArray items = null;
						try{
							items = json.getJSONArray("top_10");
							for(int i=0; i < items.length(); i ++ ){
								JSONObject obj = items.getJSONObject(i);
								UserSpot newUserSpot = UserSpot.fromSpotTopTen(spotId,  obj);
								UserSpot oldUserSpot = GowallaDBManager.getUserSpot(newUserSpot.getUser_id(), newUserSpot.getSpot_id());

								if( oldUserSpot == null  ){
									//System.out.println("INSERT " + newUserSpot);
									topTenRet = GowallaDBManager.addUserSpot( newUserSpot );
									if( topTenRet ){
										insertCount ++;
									}
								}else{
									newUserSpot.setId( oldUserSpot.getId() );
									// only need to update if the checkins count is greater
									if( oldUserSpot.getUser_checkins_count() < newUserSpot.getUser_checkins_count() ){
										//System.out.println("UPDATE " + newUserSpot);
										topTenRet = GowallaDBManager.updateUserSpot( newUserSpot );
										if( topTenRet ){
											updateCount ++;
										}
									}else{
										topTenRet = true;
										//System.out.println("No need UPDATE");
									}
								}
							}
						}catch(Exception e){}
					}

					long end = System.currentTimeMillis(); 
					long elapsedTime = end - start;
					log(NAME + " " + spotId + " : " +  getMsgFromCode(taskCode) + ", insert " + insertCount + ",update " + updateCount + ", elapsed " + elapsedTime);
					logger.info(NAME + " " + spotId + " : " + getMsgFromCode(taskCode) + ", insert " + insertCount + ",update " + updateCount + ", elapsed " + elapsedTime);

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
