package com.tamchuen.crawler.gowalla.db;

import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import com.tamchuen.crawler.db.DuplicateException;
import com.tamchuen.crawler.gowalla.domain.Spot;
import com.tamchuen.crawler.gowalla.domain.User;
import com.tamchuen.crawler.gowalla.domain.UserCheckin;
import com.tamchuen.crawler.gowalla.domain.UserFriend;
import com.tamchuen.crawler.gowalla.domain.UserSpot;
import com.tamchuen.jutil.data.DataPage;
import com.tamchuen.jutil.j4log.Logger;
import com.tamchuen.jutil.sql.DBClientWrapper;
import com.tamchuen.jutil.sql.DBPage;
import com.tamchuen.jutil.sql.DBWrapperFactory;
import com.tamchuen.jutil.sql.SimpleRowSet;
import com.tamchuen.jutil.util.FileUtil;

/**
 * DB access for Gowalla.com
 * @author Dequan
 * TODO: extract insert/update sql statement creation to ValueObject
 */
public class GowallaDBManager
{
	private static Logger systemLogger = Logger.getLogger("gowalla_crawler");
	private static final String NAME = GowallaDBManager.class.getSimpleName();
	private static DBClientWrapper DB_CONN = DBWrapperFactory.getDBClientWrapper("gowalla");

	private final static java.text.DateFormat dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


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
		StringBuilder sql = new StringBuilder("insert into " + o.getTableName() + "(id,first_name,last_name,hometown,challenge_pin_count,trip_pin_count,state_pin_count,following_count,province_pin_count,stamps_count,pins_count,country_pin_count,region_pin_count,highlights_count,trips_count,items_count,blocking_count,followers_count,friends_count,photos_count,facebook_id,twitter_id,twitter_username) values(");
		sql.append(o.getId()).append(",");
		sql.append("\"").append(o.getFirst_name()).append("\",");
		sql.append("\"").append(o.getLast_name()).append("\",");
		sql.append("\"").append(o.getHometown()).append("\",");
		sql.append(o.getChallenge_pin_count()).append(",");
		sql.append(o.getTrip_pin_count()).append(",");
		sql.append(o.getState_pin_count()).append(",");
		sql.append(o.getFollowing_count()).append(",");
		sql.append(o.getProvince_pin_count()).append(",");
		sql.append(o.getStamps_count()).append(",");
		sql.append(o.getPins_count()).append(",");
		sql.append(o.getCountry_pin_count()).append(",");
		sql.append(o.getRegion_pin_count()).append(",");
		sql.append(o.getHighlights_count()).append(",");
		sql.append(o.getTrips_count()).append(",");
		sql.append(o.getItems_count()).append(",");
		sql.append(o.getBlocking_count()).append(",");
		sql.append(o.getFollowers_count()).append(",");
		sql.append(o.getFriends_count()).append(",");
		sql.append(o.getPhotos_count()).append(",");
		sql.append(o.getFacebook_id()).append(",");
		sql.append(o.getTwitter_id()).append(",");
		sql.append("\"").append(o.getTwitter_username()).append("\"");
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
		sql.append(o.getUser1()).append(",");
		sql.append(o.getUser2());
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
	}

	public static boolean addSpot(Spot o) throws DuplicateException
	{
		if( o == null )
		{
			return false;
		}

		// insert sql
		StringBuilder sql = new StringBuilder("insert into " + o.getTableName() + "(id,name,checkins_count,created_at,max_items_count,trending_level,creator_id,users_count,lat,lng,highlights_count,photos_count1,photos_count2,items_count,description,foursquare_id,radius_meters,strict_radius,founders,spot_categories,yelp_url,street_address,city,region,country) values(");
		sql.append(o.getId()).append(",");
		sql.append("\"").append(o.getName()).append("\",");
		sql.append(o.getCheckins_count()).append(",");
		sql.append("\"").append(o.getCreated_at()).append("\",");
		sql.append(o.getMax_items_count()).append(",");
		sql.append(o.getTrending_level()).append(",");
		sql.append(o.getCreator_id()).append(",");
		sql.append(o.getUsers_count()).append(",");
		sql.append(o.getLat()).append(",");
		sql.append(o.getLng()).append(",");
		sql.append(o.getHighlights_count()).append(",");
		sql.append(o.getPhotos_count1()).append(",");
		sql.append(o.getPhotos_count2()).append(",");
		sql.append(o.getItems_count()).append(",");
		sql.append("\"").append(o.getDescription()).append("\",");
		sql.append("\"").append(o.getFoursquare_id()).append("\",");
		sql.append(o.getRadius_meters()).append(",");
		sql.append(o.getStrict_radius()).append(",");
		sql.append("\"").append(o.getFounders()).append("\",");
		sql.append("\"").append(o.getSpot_categories()).append("\",");
		sql.append("\"").append(o.getYelp_url()).append("\",");
		sql.append("\"").append(o.getStreet_address()).append("\",");
		sql.append("\"").append(o.getCity()).append("\",");
		sql.append("\"").append(o.getRegion()).append("\",");
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
		sql.append("checkins_count=").append(o.getCheckins_count()).append(",");
		sql.append("created_at=").append("\"").append(o.getCreated_at()).append("\",");
		sql.append("max_items_count=").append(o.getMax_items_count()).append(",");
		sql.append("trending_level=").append(o.getTrending_level()).append(",");
		sql.append("creator_id=").append(o.getCreator_id()).append(",");
		sql.append("users_count=").append(o.getUsers_count()).append(",");
		sql.append("lat=").append(o.getLat()).append(",");
		sql.append("lng=").append(o.getLng()).append(",");
		sql.append("highlights_count=").append(o.getHighlights_count()).append(",");
		sql.append("photos_count1=").append(o.getPhotos_count1()).append(",");
		sql.append("photos_count2=").append(o.getPhotos_count2()).append(",");
		sql.append("items_count=").append(o.getItems_count()).append(",");
		sql.append("description=").append("\"").append(o.getDescription()).append("\",");
		sql.append("foursquare_id=").append("\"").append(o.getFoursquare_id()).append("\",");
		sql.append("radius_meters=").append(o.getRadius_meters()).append(",");
		sql.append("strict_radius=").append(o.getStrict_radius()).append(",");
		sql.append("founders=").append("\"").append(o.getFounders()).append("\",");
		sql.append("spot_categories=").append("\"").append(o.getSpot_categories()).append("\",");
		sql.append("yelp_url=").append("\"").append(o.getYelp_url()).append("\",");
		sql.append("street_address=").append("\"").append(o.getStreet_address()).append("\",");
		sql.append("city=").append("\"").append(o.getCity()).append("\",");
		sql.append("region=").append("\"").append(o.getRegion()).append("\",");
		sql.append("country=").append("\"").append(o.getCountry()).append("\"");

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
		StringBuilder sql = new StringBuilder("insert into " + o.getTableName() + "(id,user_id,spot_id,created_at,message,photos_count,comments_count) values(");
		sql.append(o.getId()).append(",");
		sql.append(o.getUser_id()).append(",");
		sql.append(o.getSpot_id()).append(",");
		sql.append("\"").append(o.getCreated_at()).append("\",");
		sql.append("\"").append(o.getMessage()).append("\",");
		sql.append(o.getPhotos_count()).append(",");
		sql.append(o.getComments_count());
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
	 * get user Spot from DB
	 * @param userId
	 * @param spotId
	 * @return
	 */
	public static UserSpot getUserSpot(long userId, long spotId)
	{
		UserSpot data = null;

		try
		{
			String sql = "select id, user_id, spot_id, user_checkins_count from " + UserSpot.getTableName(userId) + " where user_id=" + userId + " and spot_id=" + spotId;
			systemLogger.debug("["+NAME+"]getUserSpot,sql:\t"+ sql);				

			SimpleRowSet rs = DB_CONN.executeQuery(sql);
			if(rs.next())
			{
				data = UserSpot.fromDB(rs);
			}
			else
			{
				systemLogger.debug("["+NAME+"]getUserSpot,user spot does not exist.");
				return null;
			}
		}
		catch (Exception e)
		{
			systemLogger.error("",e);
		}

		return data;
	}

	/**
	 * check if the user-spot exists in DB
	 * @param userId
	 * @param spotId
	 * @return  if not exists will return -1, otherwise return an ID > 0 
	 */
	public static long checkUserSpot(long userId, long spotId)
	{
		long id = -1;
		try
		{
			String sql = "select id from " + UserSpot.getTableName(userId) + " where user_id=" + userId + " and spot_id=" + spotId;

			SimpleRowSet rs = DB_CONN.executeQuery(sql);
			if(rs.next())
			{
				id = rs.getLong("id");
			}
			else
			{
				//systemLogger.debug("["+NAME+"]checkUserSpot, user spot does not exist.");
				return -1;
			}
		}
		catch (Exception e)
		{
			systemLogger.error("",e);
		}
		return id;
	}
	/**
	 * check if the user exists in DB
	 * @param userId
	 * @param spotId
	 * @return  if not exists will return -1, otherwise return an ID > 0 
	 */
	public static long checkUser(long idToCheck)
	{
		long id = -1;
		try
		{
			String sql = "select id from " + User.getTableName(idToCheck) + " where id=" + idToCheck;

			SimpleRowSet rs = DB_CONN.executeQuery(sql);
			if(rs.next())
			{
				id = rs.getLong("id");
			}
			else
			{
				//systemLogger.debug("["+NAME+"]checkUser, user " + idToCheck + " does not exist.");
				return -1;
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
	 * @param user1 Id
	 * @param user2 Id
	 * @return  if not exists will return -1, otherwise return an ID > 0 
	 */
	public static long checkUserFriend(long user1, long user2)
	{
		long id = -1;
		try
		{
			String sql = "select id from " + UserFriend.getTableName(user1) + " where user1=" + user1 + " and user2=" + user2;
			SimpleRowSet rs = DB_CONN.executeQuery(sql);
			if(rs.next())
			{
				id = rs.getLong("id");
			}
			else
			{
				//systemLogger.debug("["+NAME+"]checkUser, user " + idToCheck + " does not exist.");
				return -1;
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
	 * @return  if not exists will return -1, otherwise return an ID > 0 
	 */
	public static long checkSpot(long idToCheck)
	{
		long id = -1;
		try
		{
			String sql = "select id from " + Spot.getTableName(idToCheck) + " where id=" + idToCheck;

			SimpleRowSet rs = DB_CONN.executeQuery(sql);
			if(rs.next())
			{
				id = rs.getLong("id");
			}
			else
			{
				//systemLogger.debug("["+NAME+"]checkSpot, spot " + idToCheck + " does not exist.");
				return -1;
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
	 * @param userId
	 * @param spotId
	 * @return  if not exists will return -1, otherwise return an ID > 0 
	 */
	public static long checkCheckin(long idToCheck)
	{
		long id = -1;
		try
		{
			String sql = "select id from " + UserCheckin.getTableName(idToCheck) + " where id=" + idToCheck;

			SimpleRowSet rs = DB_CONN.executeQuery(sql);
			if(rs.next())
			{
				id = rs.getLong("id");
			}
			else
			{
				//systemLogger.debug("["+NAME+"]checkCheckin, spot " + idToCheck + " does not exist.");
				return -1;
			}
		}
		catch (Exception e)
		{
			systemLogger.error("",e);
		}
		return id;
	}
	/**
	 * get a list of users
	 * @param pageSize
	 * @param pageNo
	 * @param whereSql
	 * @return
	 */
	public static DataPage<User> getUsersPage( int pageSize, int pageNo, String tableName, String whereSql)
	{
		DataPage<User> dp = null;
		String sql = "";

		try
		{
			// create the sql string
			sql = "select id,first_name,last_name,hometown,challenge_pin_count,trip_pin_count,state_pin_count,following_count,province_pin_count,stamps_count,pins_count,country_pin_count,region_pin_count,highlights_count,trips_count,items_count,blocking_count,followers_count,friends_count,photos_count from "
					+ tableName;

			if ( !"".equals(whereSql) )
			{
				sql += " where " + whereSql;
			}

			systemLogger.debug("["+NAME+"]getUserPage,sql:\t"+ sql);	

			// execute the sql
			DBPage pg = DB_CONN.queryPage(sql, pageSize, pageNo);
			SimpleRowSet rs = pg.getRecord(); 

			// populate the list of users 
			ArrayList<User> itemList = new ArrayList<User>();
			while(rs.next())
			{
				User item = User.fromDB( rs );
				itemList.add(item);
			}

			// create the data page for return 
			dp = new DataPage<User>(itemList, pg.getTotalRecordCount(), pageSize, pageNo);

		}
		catch (Exception e)
		{
			systemLogger.error("sql:" + sql,e);
		}

		if (dp == null) {
			dp = new DataPage<User>();
		}
		return dp ;		
	}

	/**
	 * update user spot
	 * @param o
	 * @return
	 */
	public static boolean updateUserSpot(UserSpot o)
	{
		if( o == null )
		{
			return false;
		}

		// sql
		StringBuilder sql = new StringBuilder("update " + o.getTableName() + " set ");
		sql.append("user_id=").append(o.getUser_id()).append(",");
		sql.append("spot_id=").append(o.getSpot_id()).append(",");
		sql.append("user_checkins_count=").append(o.getUser_checkins_count());
		sql.append(" where id=").append(o.getId());

		try
		{
			systemLogger.debug("["+NAME+"]updateUserSpot,sql:\t"+ sql);
			DB_CONN.executeUpdate(sql.toString());
			return true;
		}
		catch(SQLException e)
		{
			systemLogger.error("sql:" + sql,e);
			return false;
		}
	}
	/**
	 * delete user spot
	 * @param id
	 * @return
	 */
	public static boolean delUserSpot(long id)
	{
		if( id <= 0)
		{
			return false;
		}
		return true;
	}

	
	private static int indent = 0;
	/**
	 * print and insert spot categories 
	 * @param rs
	 * @param parentId
	 * @throws Exception
	 */
	public static void printSpotCategories(JSONObject rs, String parentId) throws Exception{
		String id = "0";
		indent++;
		
		if( rs.has("url")){
			id = rs.getString("url").replaceAll("/categories/", "");
			String name = rs.getString("name");
			String desc = rs.getString("description");
			for(int i=0; i < indent; i ++ ){
				//System.out.print("   ");
			}
			//System.out.println("ID:" + id+ ", Parent:" + parentId + ", Name:" + name + ", Desc:" + desc);
			//System.out.println( id+":" + name );
			String sql = "insert into spotCategories(cat_id,parent_id,name,description) values(" + id+ "," + parentId + ",\"" + name + "\", \"" + desc + "\");";
			System.out.println(sql);
			/*
			try
			{
				DB_CONN.executeUpdate(sql.toString());
			}
			catch(SQLException e)
			{
				e.printStackTrace();
			}*/
		}
		
		if( rs.has("spot_categories")){
			JSONArray items = rs.getJSONArray("spot_categories" );
		    for(int i=0; i < items.length(); i ++ ){
		    	printSpotCategories( items.getJSONObject(i), id );
		    }
			
		}
		
		indent--;
	}
	
	public static void getAllUserSpots(){
		Long id = -1L;
		List<String> ids = new ArrayList<String>();
		PrintWriter out = null;
		try
		{
			out = new PrintWriter("C:\\Users\\Dequan\\Desktop\\CAI-Paper\\allUserSpots", "UTF-8");
			// get ids from spots
			ids = FileUtil.readFile2List("C:\\Users\\Dequan\\Desktop\\CAI-Paper\\allSpots");
			
			System.out.println("Get " + ids.size() + " IDs");
			out.append("user_id,spot_id,user_checkins_count\r\n");
			for(String spot_id : ids){
				// get user spots
				for(int i=1; i <=5 ; i++){
					String sql = "select id,user_id,spot_id,user_checkins_count from userspots" + i + " where spot_id= " + spot_id;
					DBPage pg = DB_CONN.queryPage(sql, 10000, 1);
					SimpleRowSet rs = pg.getRecord();
					if(rs.next())
					{
						UserSpot us = UserSpot.fromDB(rs);
						out.append(us.getUser_id() + "," + us.getSpot_id() + "," + us.getUser_checkins_count() + "\r\n" );
					}
					else
					{
						System.err.println("Error SQL:" + sql);
					}
				}
			}
			out.flush();
			
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		finally{
			out.close();
		}
		
		
	}
	
	public static void main(String[] args){

		/*
	// get users list from DB
	DataPage<User> pg = SiteManager.getUsersPage(10, 1, "");
	List<User> itemList = pg.getRecord();

	long id;
	String name;

	for (int i = 0; i < itemList.size(); i++) {
	    User item = itemList.get(i);
	    id = item.getId();
	    name = item.getFirst_name();
	    System.out.println("ID:" + id + ",Name:" + name );
	}
		 */


		Map<String, String> headers = new HashMap<String,String>();
		headers.put("Accept", "application/json");
		headers.put("X-Gowalla-API-Key", "16fcc915d11a419ba98c4ab062c19930");

		String result = "";
		boolean ret = false;
		String GOWALLA_USERS_URL = "http://api.gowalla.com/users/";
		String GOWALLA_SPOTS_URL = "http://api.gowalla.com/spots/";
		String GOWALLA_CHECKINS_URL = "http://api.gowalla.com/checkins/";
		String GOWALLA_CATEGORIES_URL = "http://api.gowalla.com/categories";
		
		long userId = 3; 
		long spotId = 9031;
		long checkinId = 101;
		String getUser = GOWALLA_USERS_URL + userId;
		String getUserFriends = GOWALLA_USERS_URL + userId + "/friends";
		String getUserVisitedSpots = GOWALLA_USERS_URL + userId + "/visited_spots_urls";
		String getSpot = GOWALLA_SPOTS_URL + spotId;
		String getCheckin = GOWALLA_CHECKINS_URL + checkinId;
		String getUserTopSpots = GOWALLA_USERS_URL + userId + "/top_spots";

		System.out.println( UserFriend.getTableName( 1005 ) );
		try {


			// test get user detail
			/*
	    result = HttpUtil.requestByGet( getUser, null, headers );
	    JSONObject json = new JSONObject( result );
	    User item = User.fromJSON( userId, json );
	    ret = SiteManager.addUser( item );
			 */
			// test get user friend
			/*
	    result = HttpUtil.requestByGet( getUserFriends, null, headers );
	    JSONObject json = new JSONObject( result );

	    JSONArray items = json.getJSONArray("users" );
	    for(int i=0; i < items.length(); i ++ ){
		JSONObject obj = items.getJSONObject(i);
		UserFriend item = UserFriend.fromJSON(userId,  obj);
		ret = SiteManager.addUserFriend( item );
	    }*/

			// test get user visited spots
			/*
	    result = HttpUtil.requestByGet( getUserVisitedSpots, null, headers );
	    JSONObject json = new JSONObject( result );
	    System.out.println(json.toString(2));

	    JSONArray items = json.getJSONArray("urls" );
	    for(int i=0; i < items.length(); i ++ ){
		String obj = items.getString(i);
		UserSpot item = UserSpot.fromVisitedSpots(userId,  obj);
		//System.out.println( item );
		ret = SiteManager.addUserSpot( item );
	    }*/

			// test get spot detail
			/*
	    result = HttpUtil.requestByGet( getSpot, null, headers );
	    JSONObject json = new JSONObject( result );
	    Spot item = Spot.fromJSON( spotId, json );
	    SiteManager.addSpot( item );
	    JSONArray items = json.getJSONArray("top_10" );
	    for(int i=0; i < items.length(); i ++ ){
		JSONObject obj = items.getJSONObject(i);
		UserSpot newUserSpot = UserSpot.fromSpotTopTen(spotId,  obj);
		UserSpot oldUserSpot = SiteManager.getUserSpot(newUserSpot.getUser_id(), newUserSpot.getSpot_id());

		if( oldUserSpot == null  ){
		    System.out.println("INSERT");
		    ret = SiteManager.addUserSpot( newUserSpot );
		}else{
		    newUserSpot.setId( oldUserSpot.getId() );
		    // only need to update if the checkins count is greater
		    if( oldUserSpot.getUser_checkins_count() < newUserSpot.getUser_checkins_count() ){
			System.out.println("UPDATE for id " + newUserSpot.getId());
			ret = SiteManager.updateUserSpot( newUserSpot );
		    }else{
			ret = true;
			System.out.println("No need UPDATE");
		    }
		}
	    }*/


			// test get checkin detail
			/*
	    result = HttpUtil.requestByGet( getCheckin, null, headers );
	    JSONObject json = new JSONObject( result );
	    UserCheckin item = UserCheckin.fromJSON( checkinId, json );
	    ret = SiteManager.addUserCheckin( item );*/

			// test get user top spots
			/*
	    result = HttpUtil.requestByGet( getUserTopSpots, null, headers );
	    JSONObject json = new JSONObject( result );

	    JSONArray items = json.getJSONArray("top_spots" );
	    for(int i=0; i < items.length(); i ++ ){
		JSONObject obj = items.getJSONObject(i);
		UserSpot newUserSpot = UserSpot.fromUserTopSpots(userId,  obj);
		UserSpot oldSpot = SiteManager.getUserSpot(newUserSpot.getUser_id(), newUserSpot.getSpot_id());

		if( oldSpot == null  ){
		    System.out.println("INSERT");
		    ret = SiteManager.addUserSpot( newUserSpot );
		}else{
		    newUserSpot.setId( oldSpot.getId() );
		    // only need to update if the checkins count is greater
		    if( oldSpot.getUser_checkins_count() < newUserSpot.getUser_checkins_count() ){
			System.out.println("UPDATE");
			ret = SiteManager.updateUserSpot( newUserSpot );
		    }else{
			ret = true;
			System.out.println("No need UPDATE");
		    }

		}
	    }
			 */
			
			// test get all categories
			/*
			String szCategories = FileUtil.readFile2String("data/gowalla/spotCategories.json", "UTF-8");
			// System.out.println(szCategories);
			JSONObject obj = new JSONObject( szCategories );
			printSpotCategories(obj, "0");*/
			
			
			// System.out.println(json.toString(2));
			
			// get all the user-checkin for 142K spots
			getAllUserSpots();
			
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}


		System.out.println("Result:" + ret);
	}

}
