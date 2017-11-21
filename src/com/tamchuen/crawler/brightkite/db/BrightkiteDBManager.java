package com.tamchuen.crawler.brightkite.db;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import com.tamchuen.crawler.brightkite.domain.Spot;
import com.tamchuen.crawler.brightkite.domain.User;
import com.tamchuen.crawler.brightkite.domain.UserCheckin;
import com.tamchuen.crawler.brightkite.domain.UserFriend;
import com.tamchuen.crawler.db.DuplicateException;
import com.tamchuen.crawler.util.HttpUtil;
import com.tamchuen.jutil.data.DataPage;
import com.tamchuen.jutil.j4log.Logger;
import com.tamchuen.jutil.sql.DBClientWrapper;
import com.tamchuen.jutil.sql.DBPage;
import com.tamchuen.jutil.sql.DBWrapperFactory;
import com.tamchuen.jutil.sql.SimpleRowSet;

/**
 * DB access for brightkite.com
 * @author Dequan
 *
 */
public class BrightkiteDBManager
{
	private static Logger systemLogger = Logger.getLogger("brightkite_crawler");
	private static final String NAME = BrightkiteDBManager.class.getSimpleName();
	private static DBClientWrapper DB_CONN = DBWrapperFactory.getDBClientWrapper("brightkite");

	/**
	 * Add user
	 * @param o
	 * @return
	 */
	public static boolean addUser(User o) throws DuplicateException
	{
		if( o == null )
		{
			return false;
		}

		// insert sql
		StringBuilder sql = new StringBuilder("insert into " + o.getTableName() + "(id,fullname,login,description,tag_list,sex,age,photos_count,friends_count,notes_count,checkins_count,fans_count,comments_count,social_links) values(");
		sql.append("\"").append(o.getId()).append("\",");
		sql.append("\"").append(o.getFullname()).append("\",");
		sql.append("\"").append(o.getLogin()).append("\",");
		sql.append("\"").append(o.getDescription()).append("\",");
		sql.append("\"").append(o.getTag_list()).append("\",");
		sql.append("\"").append(o.getSex()).append("\",");
		sql.append(o.getAge()).append(",");
		sql.append(o.getPhotos_count()).append(",");
		sql.append(o.getFriends_count()).append(",");
		sql.append(o.getNotes_count()).append(",");
		sql.append(o.getCheckins_count()).append(",");
		sql.append(o.getFans_count()).append(",");
		sql.append(o.getComments_count()).append(",");
		sql.append("\"").append(o.getSocial_links()).append("\"");
		sql.append(")");
		try
		{
			systemLogger.debug("["+NAME+"]addUser,sql:\t"+ sql);
			DB_CONN.executeUpdate(sql.toString());
		}
		catch(SQLException e)
		{
			systemLogger.error("sql:" + sql,e);
			if( e.getMessage().indexOf("Duplicate") > -1 ){
				throw new DuplicateException();
			}else{
				return false;
			}
		}

		return true;
	}

	/**
	 * Add user friend
	 * @param o
	 * @return
	 */
	public static boolean addUserFriend(UserFriend o) throws DuplicateException
	{
		if( o == null )
		{
			return false;
		}

		// insert sql
		StringBuilder sql = new StringBuilder("insert into " + o.getTableName() + "(user1, user2) values(");
		sql.append("\"").append(o.getUser1()).append("\",");
		sql.append("\"").append(o.getUser2()).append("\"");
		sql.append(")");
		try
		{
			systemLogger.debug("["+NAME+"]addUserFriend,sql:\t"+ sql);
			DB_CONN.executeUpdate(sql.toString());
		}
		catch(SQLException e)
		{
			systemLogger.error("sql:" + sql,e);
			if( e.getMessage().indexOf("Duplicate") > -1 ){
				throw new DuplicateException();
			}else{
				return false;
			}
		}

		return true;
	}

	/**
	 * Add user spot
	 * @param o
	 * @return
	 */
	/*
	public static boolean addUserSpot(UserSpot o) throws DuplicateException
	{
		if( o == null )
		{
			return false;
		}

		// insert sql
		StringBuilder sql = new StringBuilder("insert into " + o.getTableName() + "(user_id, spot_id, user_checkins_count) values(");
		sql.append(o.getUser_id()).append(",");
		sql.append(o.getSpot_id()).append(",");
		sql.append(o.getUser_checkins_count());
		sql.append(")");

		try
		{
			systemLogger.debug("["+NAME+"]addUserSpot,sql:\t"+ sql);
			DB_CONN.executeUpdate(sql.toString());
		}
		catch(SQLException e)
		{
			systemLogger.error("sql:" + sql,e);
			if( e.getMessage().indexOf("Duplicate") > -1 ){
				throw new DuplicateException();
			}else{
				return false;
			}
		}

		return true;
	}*/

	public static boolean addSpot(Spot o) throws DuplicateException
	{
		if( o == null )
		{
			return false;
		}

		// insert sql
		StringBuilder sql = new StringBuilder("insert into " + o.getTableName() + "(id,name,latitude,longitude,attribution,scope,street,city,state,country) values(");
		sql.append("\"").append(o.getId()).append("\",");
		sql.append("\"").append(o.getName()).append("\",");
		sql.append(o.getLatitude()).append(",");
		sql.append(o.getLongitude()).append(",");
		sql.append("\"").append(o.getAttribution()).append("\",");
		sql.append("\"").append(o.getScope()).append("\",");
		sql.append("\"").append(o.getStreet()).append("\",");
		sql.append("\"").append(o.getCity()).append("\",");
		sql.append("\"").append(o.getState()).append("\",");
		sql.append("\"").append(o.getCountry()).append("\"");
		sql.append(")");

		try
		{
			systemLogger.debug("["+NAME+"]addSpot,sql:\t"+ sql);
			DB_CONN.executeUpdate(sql.toString());
		}
		catch(SQLException e)
		{
			systemLogger.error("sql:" + sql,e);
			if( e.getMessage().indexOf("Duplicate") > -1 ){
				throw new DuplicateException();
			}else{
				return false;
			}
		}

		return true;
	}
	/**
	 * update spot information
	 * @param o
	 * @return
	 */
	public static boolean updateSpot(Spot o)
	{
		if( o == null )
		{
			return false;
		}

		// update sql
		StringBuilder sql = new StringBuilder("update " + o.getTableName() + " set ");
		sql.append("name=").append("\"").append(o.getName()).append("\",");
		sql.append("latitude=").append(o.getLatitude()).append(",");
		sql.append("longitude=").append(o.getLongitude()).append(",");
		sql.append("attribution=").append("\"").append(o.getAttribution()).append("\",");
		sql.append("scope=").append("\"").append(o.getScope()).append("\",");
		sql.append("street=").append("\"").append(o.getStreet()).append("\",");
		sql.append("city=").append("\"").append(o.getCity()).append("\",");
		sql.append("state=").append("\"").append(o.getState()).append("\",");
		sql.append("country=").append("\"").append(o.getCountry()).append("\",");

		sql.append(" where id=").append(o.getId());

		try
		{
			systemLogger.debug("["+NAME+"]updateSpot,sql:\t"+ sql);
			DB_CONN.executeUpdate(sql.toString());
		}
		catch(SQLException e)
		{
			systemLogger.error("sql:" + sql,e);
			return false;

		}

		return true;
	}

	/**
	 * Add user Checkin
	 * @param o
	 * @return
	 */
	public static boolean addUserCheckin(UserCheckin o) throws DuplicateException
	{
		if( o == null )
		{
			return false;
		}

		// insert sql
		StringBuilder sql = new StringBuilder("insert into " + o.getTableName() + "(id,user_id,spot_id,created_at,via,rating,ratings_count,comments_count,view_count) values(");
		sql.append("\"").append(o.getId()).append("\",");
		sql.append("\"").append(o.getUser_id()).append("\",");
		sql.append("\"").append(o.getSpot_id()).append("\",");
		sql.append("\"").append(o.getCreated_at()).append("\",");
		sql.append("\"").append(o.getVia()).append("\",");
		sql.append(o.getRating()).append(",");
		sql.append(o.getRatings_count()).append(",");
		sql.append(o.getComments_count()).append(",");
		sql.append(o.getView_count());
		sql.append(")");

		try
		{
			systemLogger.debug("["+NAME+"]addUserCheckin,sql:\t"+ sql);
			DB_CONN.executeUpdate(sql.toString());
		}
		catch(SQLException e)
		{
			systemLogger.error("sql:" + sql,e);
			if( e.getMessage().indexOf("Duplicate") > -1 ){
				throw new DuplicateException();
			}else{
				return false;
			}
		}

		return true;
	}

	/**
	 * check if the user exists in DB
	 * @param userId
	 * @param spotId
	 * @return  if not exists will return null, otherwise return an ID > 0 
	 */
	public static String checkUser(String idToCheck)
	{
		String id = null;
		try
		{
			String sql = "select id from " + User.getTableName(idToCheck) + " where id=\"" + idToCheck + "\"";

			SimpleRowSet rs = DB_CONN.executeQuery(sql);
			if(rs.next())
			{
				id = rs.getString("id");
			}
			else
			{
				//systemLogger.debug("["+NAME+"]checkUser, user " + idToCheck + " does not exist.");
				return null;
			}
		}
		catch (Exception e)
		{
			systemLogger.error("",e);
		}
		return id;
	}
	/**
	 * check if the spot exists in DB
	 * @param userId
	 * @param spotId
	 * @return  if not exists will return null, otherwise return an ID > 0 
	 */
	public static String checkSpot(String idToCheck)
	{
		String id = null;
		try
		{
			String sql = "select id from " + Spot.getTableName(idToCheck) + " where id=\"" + idToCheck + "\"";

			SimpleRowSet rs = DB_CONN.executeQuery(sql);
			if(rs.next())
			{
				id = rs.getString("id");
			}
			else
			{
				//systemLogger.debug("["+NAME+"]checkSpot, spot " + idToCheck + " does not exist.");
				return null;
			}
		}
		catch (Exception e)
		{
			systemLogger.error("",e);
		}
		return id;
	}
	/**
	 * check if the checkin exists in DB
	 * @param checkinId
	 * @param createdTime
	 * @return  if not exists will return null, otherwise return an ID > 0 
	 */
	public static String checkCheckin(String idToCheck, String created_at)
	{
		String id = null;
		try
		{
			String sql1 = "select id from " + UserCheckin.getTableName(created_at) + " where id=\"" + idToCheck + "\"";

			SimpleRowSet rs1 = DB_CONN.executeQuery(sql1);
			if(rs1.next())
			{
				id = rs1.getString("id");
			}
			else
			{
				//systemLogger.debug("["+NAME+"]checkCheckin, checkin " + idToCheck + " does not exist.");
				return null;
				
			}
		}
		catch (Exception e)
		{
			systemLogger.error("",e);
		}
		return id;
	}
	
	/**
	 * check if the user friend exists in DB
	 * @param userId
	 * @param spotId
	 * @return  if not exists will return null, otherwise return an ID > 0 
	 */
	public static String checkUserFriend(String id1, String id2)
	{
		String id = null;
		try
		{
			String sql = "select id from " + UserFriend.getTableName(id1) + " where user1=\"" + id1 + "\" and user2=\"" + id2 + "\"";
			SimpleRowSet rs = DB_CONN.executeQuery(sql);
			if(rs.next())
			{
				id = rs.getString("id");
			}
			else
			{
				//systemLogger.debug("["+NAME+"]checkUserFriend, " + id1 + "," + id2  + " does not exist.");
				return null;
			}
		}
		catch (Exception e)
		{
			systemLogger.error("",e);
		}
		return id;
	}
	
	public static DataPage<UserCheckin> getCheckinsPage( int pageSize, int pageNo, String tableName, String whereSql)
	{
		DataPage<UserCheckin> dp = null;
		String sql = "";

		try
		{
			// create the sql string
			sql = "select id,user_id,spot_id,created_at,via,rating,ratings_count,comments_count,view_count from "
					+ tableName;

			if (whereSql != null &&  ! "".equals(whereSql) )
			{
				sql += " where " + whereSql;
			}

			systemLogger.debug("["+NAME+"]getUserPage,sql:\t"+ sql);	

			// execute the sql
			DBPage pg = DB_CONN.queryPage(sql, pageSize, pageNo);
			SimpleRowSet rs = pg.getRecord(); 

			// populate the list of users 
			ArrayList<UserCheckin> itemList = new ArrayList<UserCheckin>();
			while(rs.next())
			{
				UserCheckin item = UserCheckin.fromDB( rs );
				itemList.add(item);
			}

			// create the data page for return 
			dp = new DataPage<UserCheckin>(itemList, pg.getTotalRecordCount(), pageSize, pageNo);

		}
		catch (Exception e)
		{
			systemLogger.error("sql:" + sql,e);
		}

		if (dp == null) {
			dp = new DataPage<UserCheckin>();
		}
		
		return dp ;		
	}
	
	/**
	 * delete user checkin 
	 * @param id
	 * @return
	 */
	public static boolean delUserCheckin(UserCheckin o)
	{
		if( o == null )
		{
			return false;
		}

		// delete sql
		String sql = "delete from " + UserCheckin.getTableName(null) + " where id=\"" + o.getId() + "\"";

		try
		{
			systemLogger.debug("["+NAME+"]delUserCheckin,sql:\t"+ sql);
			DB_CONN.executeUpdate(sql.toString());
		}
		catch(SQLException e)
		{
			systemLogger.error("sql:" + sql,e);
			return false;
		}

		return true;
	}
	/**
	 * Distribute the user checkins records into different tables
	 */
	public static void distributeUserCheckins(int totalRecords, int pageSize){
		int iPages = totalRecords/pageSize + 1;
		long start = System.currentTimeMillis();
		int totalPages = 0;
		for(int i =0; i < iPages ; i ++ ){
			DataPage<UserCheckin> dp = getCheckinsPage(pageSize, 1 , UserCheckin.getTableName(null), null);
			List<UserCheckin> items = dp.getRecord();
			System.out.println("Record Count:" + dp.getRecordCount() );
			if( dp.getRecordCount() == 0 ){
				break;
			}
			totalPages ++;
			
			for(UserCheckin item : items ){
				//System.out.println(  item.getCreated_at() + ":" + item.getTableName() );
				boolean ret1 = false;
				boolean ret2= false;
				try {
					ret1 = addUserCheckin( item );
				} catch (DuplicateException e) {
					e.printStackTrace();
				}
				
				if( ret1 ){
					ret2 = delUserCheckin( item );
				}
				if( i% 100 ==0 ){
					System.out.println(  item.getId() + "," + ret1 + "," + ret2 + "," + item.getTableName());
				}
			}
			//sleep();
		}
		long end = System.currentTimeMillis();
		long time = (end-start)/1000;
		System.out.println( "Total Pages " + totalPages + ", time:" + time + "s");
	}
	
	private static void sleep(){
		try
		{
			Thread.sleep(5000);
		}
		catch (InterruptedException e)
		{
		}	
	}
	
	public static void main(String[] args){
		String result = "";
		boolean ret = false;
		String USERS_URL = "http://brightkite.com/people/";
		String SPOTS_URL = "http://brightkite.com/places/";
		String OBJECTS_URL = "http://brightkite.com/objects.json";

		String userId = "af96a2fa9a5211dd9951003048c10834";
		String userLogin = "jeffhinz"; 
		String spotId = "c93132db8758c16ebffae440b6ee7369f63064b9";
		String checkinId = "985e5d555fe8ec80abb08c8cc5006a2bbd9f8d21";
		
		String getUser = USERS_URL + userId + ".json";
		String getUserFriends = USERS_URL + userId + "/friends.json";
		String getUserVisitedSpots = OBJECTS_URL + "?offset=0&limit=100&person_id=" + userId;
		String getSpot = SPOTS_URL + spotId + ".json";
		String getCheckinsFromLatLngUrl = OBJECTS_URL + "?filters=checkins&offset=0&limit=100&latitude=40.7008291704&longitude=-74.0130579472&radius=1000";

		JSONObject json = null;
		int pageSize = 100;
		int timeout = 5000;
		
		try {
			//System.out.println( new BASE64Encoder().encode("jeffhinz".getBytes()) );
			//System.out.println(  MD5Coding.encode2HexStr("jeffhinz".getBytes()) );
			
			// test get user detail
			/*
			String oldUserId = BrightkiteDBManager.checkUser( userId);
			if( oldUserId == null ){
				getUser = USERS_URL + userId + ".json";
				System.out.println("Req : Get User, " + getUser);
				result = HttpUtil.requestByGet( getUser, timeout );
				json = new JSONObject( result );
				User user = User.fromJSON( userId, json );
				ret = BrightkiteDBManager.addUser( user );
				System.out.println("Insert User: " + ret);
				
				sleep();
			}else{
				System.out.println("User Exists: " + oldUserId);
			}
			*/
			
			
			// test get user friend
			/*
			//int friendsCount = 	user.getFriends_count();
			int friendsCount = 105;
			if( friendsCount > 0 ){
				int noOfPages = friendsCount/pageSize + 1;
				System.out.println("Total Count:" +friendsCount + ", pageSize:" + pageSize + ", pages:" + noOfPages );
				for(int p=0; p < noOfPages; p++ ){
					getUserFriends = USERS_URL + userId + "/friends.json?offset=" + (p*pageSize) + "&limit=" + pageSize;
					System.out.println("Req : Get User Friends " + (p+1) + ", " + getUserFriends);
					result = HttpUtil.requestByGet( getUserFriends, timeout );

					JSONArray friendsJson = new JSONArray( result );
					for(int i=0; i < friendsJson.length(); i ++ ){
						JSONObject obj = friendsJson.getJSONObject(i);
						UserFriend item = UserFriend.fromJSON(userId,  obj);
						ret = BrightkiteDBManager.addUserFriend( item );
						System.out.println("Get User Friends:" + ret);
					}
					sleep();
				}
			}*/

			// test search checkins by latlng
			/*
			String lat = "40.7008291704";
			String lng = "-74.0130579472";
			int radius = 1000;
			
			getCheckinsFromLatLngUrl = OBJECTS_URL + "?filters=checkins&latitude=" + lat + "&longitude=" + lng + "&radius=" + radius + "&limit=" + pageSize;
			System.out.println("Req : Get Checkins, " + getCheckinsFromLatLngUrl);
			result = HttpUtil.requestByGet( getCheckinsFromLatLngUrl , timeout );

			JSONArray checkinsJson = new JSONArray( result );
			int checkinInsertCount = 0;
			for(int i=0; i < checkinsJson.length(); i ++ ){
				JSONObject obj = checkinsJson.getJSONObject(i);
				UserCheckin item = UserCheckin.fromJSON( obj );
				String oldCheckinId = BrightkiteDBManager.checkCheckin(item.getId());
				if( oldCheckinId == null ){
					ret = BrightkiteDBManager.addUserCheckin( item );
					System.out.println("Insert Checkin: " + ret);
					if( ret ){
						checkinInsertCount ++;
					}
				}else{
					System.out.println("Checkin Exists: " + oldCheckinId);
				}
				
			}
			System.out.println("Get Checkins:" + checkinInsertCount + "/" + checkinsJson.length() );
			sleep();*/
			
			// test search checkins by user ID
			/*
			int userCheckinsCount = 105;
			if( userCheckinsCount > 0 ){
				int noOfPages = userCheckinsCount/pageSize + 1;
				System.out.println("Total Count:" + userCheckinsCount + ", pageSize:" + pageSize + ", pages:" + noOfPages );
				for(int p=0; p < noOfPages; p++ ){
					String getCheckinsFromUserUrl = OBJECTS_URL + "?filters=checkins&person_id=" + userId + "&offset=" + (p*pageSize) + "&limit=" + pageSize;
					System.out.println("Req : Get Checkins, " + getCheckinsFromUserUrl);
					result = HttpUtil.requestByGet( getCheckinsFromUserUrl , timeout );

					JSONArray checkinsJson = new JSONArray( result );
					int checkinInsertCount = 0;
					for(int i=0; i < checkinsJson.length(); i ++ ){
						JSONObject obj = checkinsJson.getJSONObject(i);
						UserCheckin item = UserCheckin.fromJSON( obj );
						String oldCheckinId = BrightkiteDBManager.checkCheckin(item.getId(), item.getCreated_at());
						if( oldCheckinId == null ){
							ret = BrightkiteDBManager.addUserCheckin( item );
							System.out.println("Insert Checkin: " + ret);
							if( ret ){
								checkinInsertCount ++;
							}
						}else{
							System.out.println("Checkin Exists: " + oldCheckinId);
						}
						
					}
					System.out.println("Get Checkins:" + checkinInsertCount + "/" + checkinsJson.length() );
					sleep();
				}
			}*/
			
			// test search checkins by spot ID
			/*
			int pageOfCheckinsFromSpot = 0;
			boolean hasNextPage = true;
			while( hasNextPage ){
				String getCheckinsFromSpotUrl = OBJECTS_URL + "?filters=checkins&place_id=" + spotId + "&offset=" + (pageOfCheckinsFromSpot*pageSize) + "&limit=" + pageSize;
				System.out.println("Req : Get Checkins, " + getCheckinsFromSpotUrl);
				result = HttpUtil.requestByGet( getCheckinsFromSpotUrl , timeout );

				JSONArray checkinsJson = new JSONArray( result );
				int checkinInsertCount = 0;
				for(int i=0; i < checkinsJson.length(); i ++ ){
					JSONObject obj = checkinsJson.getJSONObject(i);
					UserCheckin item = UserCheckin.fromJSON( obj );
					String oldCheckinId = BrightkiteDBManager.checkCheckin(item.getId(), item.getCreated_at());
					if( oldCheckinId == null ){
						ret = BrightkiteDBManager.addUserCheckin( item );
						System.out.println("Insert Checkin: " + ret);
						if( ret ){
							checkinInsertCount ++;
						}
					}else{
						System.out.println("Checkin Exists: " + oldCheckinId);
					}
				}
				
				if(  checkinsJson.length() < pageSize ){
					hasNextPage = false;
				}
				
				pageOfCheckinsFromSpot ++;
				System.out.println("Get Checkins:" + checkinInsertCount + "/" + checkinsJson.length() );
				sleep();
			}*/
			
			// test get spot detail
			/*
			String oldSpotId = BrightkiteDBManager.checkSpot( spotId);
			if( oldSpotId == null ){
				getSpot = SPOTS_URL + spotId + ".json";
				System.out.println("Req : Get Spot, " + getSpot);
				result = HttpUtil.requestByGet( getSpot, timeout );
				json = new JSONObject( result );
				Spot spot = Spot.fromJSON( spotId, json );
				ret = BrightkiteDBManager.addSpot( spot );
				
				System.out.println("Insert Spot: " + ret);
				sleep();
			}else{
				System.out.println("User Exists: " + oldSpotId);
			}
			*/

			// as of 2011 12/12, total checkins is 728483
			// distributeUserCheckins( 10, 100 );
			System.out.println(checkCheckin("f76ada8879d711dd8b860030487eb504", "2008/07/02"));
			//System.out.println(json.toString(2));
		} catch (Exception e) {
			e.printStackTrace();
		}


		System.out.println("Result:" + ret);
	}

}
