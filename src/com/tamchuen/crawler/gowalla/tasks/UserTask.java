package com.tamchuen.crawler.gowalla.tasks;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.json.JSONObject;

import com.tamchuen.crawler.Config;
import com.tamchuen.crawler.db.DuplicateException;
import com.tamchuen.crawler.gowalla.db.GowallaDBManager;
import com.tamchuen.crawler.gowalla.domain.User;
import com.tamchuen.crawler.util.HttpUtil;
import com.tamchuen.jutil.j4log.Logger;
import com.tamchuen.jutil.util.FileUtil;
import com.tamchuen.jutil.util.Pair;

/**
 * Get User detail information
 * @author Dequan
 *
 */
public class UserTask extends GowallaTask {
	public static final String NAME = "UserTask";
	private static final Logger logger = Logger.getLogger( NAME );
	private static int POLL_INTERVAL = 5000;

	public UserTask() {
		this.setName(NAME);
	}

	public boolean start(){
		// set status as started
		super.start();

		// calculate where to start
		final Config config = this.getConfig();

		new Thread(NAME+".background")
		{
			@Override
			public void run()
			{
				logger.info(NAME + ".start|start background thread:" + Thread.currentThread());

				String id = null;

				// loop
				while ( (id = taskIdQueue.poll()) != null && TaskStatus.STARTED.equals( UserTask.this.getStatus()) )
				{
					long start = System.currentTimeMillis();
					long userId = Long.valueOf(id);
					currentId = id;

					String apiUrl = GOWALLA_USERS_URL + id;
					String httpResult = null;
					TaskCode taskCode = TaskCode.UNKNOWN;
					boolean dbResult = false;

					// check DB first
					long oldId = GowallaDBManager.checkUser(userId);
					if( oldId > 0 ){
						// exists in DB
						taskCode = TaskCode.EXISTS;
						log(NAME + " " + userId + " : " +  getMsgFromCode(taskCode) );
						continue;
					}   

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
						logger.error(NAME + ".background thread HTTP exception", e);
						taskCode = TaskCode.HTTP_ERROR;
					}

					// Process HTTP content
					if( httpResult != null ){
						try
						{
							JSONObject json = new JSONObject( httpResult );
							User item = User.fromJSON( userId, json );
							dbResult = GowallaDBManager.addUser( item );
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


					long end = System.currentTimeMillis(); 
					long elapsedTime = end - start;

					// TODO: why simple log has not effect
					//log("logger simple: " + missedLogger.isSimple());
					log(NAME + " " + userId + " : " +  getMsgFromCode(taskCode) + ", elapsed " + elapsedTime);
					logger.info(NAME + " " + userId + " : " + getMsgFromCode(taskCode) + ", elapsed " + elapsedTime);

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
