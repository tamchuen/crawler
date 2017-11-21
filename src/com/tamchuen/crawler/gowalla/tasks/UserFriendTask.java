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
import com.tamchuen.crawler.gowalla.domain.UserFriend;
import com.tamchuen.crawler.tasks.AbstractTask.TaskCode;
import com.tamchuen.crawler.util.HttpUtil;
import com.tamchuen.jutil.j4log.Logger;
import com.tamchuen.jutil.util.Pair;

/**
 * Get User Friend relationship information
 * @author Dequan
 *
 */
public class UserFriendTask extends GowallaTask {
	public static final String NAME = "UserFriendTask";
	private static final Logger logger = Logger.getLogger( NAME );
	private static int POLL_INTERVAL = 5000;

	public UserFriendTask() {
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
				while ( (id = taskIdQueue.poll()) != null && TaskStatus.STARTED.equals( UserFriendTask.this.getStatus()) )
				{
					long start = System.currentTimeMillis();
					long userId = Long.valueOf(id);
					currentId = id;

					String apiUrl = GOWALLA_USERS_URL + userId + "/friends";
					String httpResult = null;

					TaskCode taskCode = TaskCode.UNKNOWN;
					boolean dbResult = false;
					List<UserFriend> dataList = new ArrayList<UserFriend>();

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
							JSONArray items = json.getJSONArray("users" );
							for(int i=0; i < items.length(); i ++ ){
								JSONObject obj = items.getJSONObject(i);
								UserFriend item = UserFriend.fromJSON(userId,  obj);
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
								// Check duplicate when inserting
								long oldId =  GowallaDBManager.checkUserFriend(dataList.get(i).getUser1(), dataList.get(i).getUser2());
								
								if( oldId == -1 ){
									dbResult = GowallaDBManager.addUserFriend( dataList.get(i) );
									if( dbResult ){
										insertCount ++;
									}
									// check result
									taskCode = dbResult ? TaskCode.SUCCESS : TaskCode.DB_ERROR;
								}else{
									taskCode = TaskCode.EXISTS;
								}
							}		
						}
						catch (DuplicateException e)
						{
							taskCode = TaskCode.DB_DUPLICATE;
						}
					} 
					
					long end = System.currentTimeMillis(); 
					long elapsedTime = end - start;

					// if it's UNKOWN, that means that user has no friends
					log(NAME + " " + userId + " : " +  getMsgFromCode(taskCode) + ", friend count:"+ insertCount + "/"+ dataList.size() + ", elapsed " + elapsedTime );
					logger.info(NAME + " " + userId + " : " +  getMsgFromCode(taskCode)+ ", friend count:"+ insertCount + "/" + dataList.size() + ", elapsed " + elapsedTime);

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
