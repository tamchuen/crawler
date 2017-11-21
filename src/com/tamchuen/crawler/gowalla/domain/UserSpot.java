package com.tamchuen.crawler.gowalla.domain;

import org.json.JSONObject;

import com.tamchuen.jutil.sql.SimpleRowSet;

public class UserSpot extends GowallaObject{
	public static final String TABLE = "userSpots";
	public static final int NO_OF_TABLES = 5;

	private long user_id;

	private long spot_id;

	private int user_checkins_count;


	public long getUser_id() {
		return user_id;
	}
	public void setUser_id(long user_id) {
		this.user_id = user_id;
	}
	public long getSpot_id() {
		return spot_id;
	}
	public void setSpot_id(long spot_id) {
		this.spot_id = spot_id;
	}
	public int getUser_checkins_count() {
		return user_checkins_count;
	}
	public void setUser_checkins_count(int user_checkins_count) {
		this.user_checkins_count = user_checkins_count;
	}
	public String toString(){
		return "{id=" + this.getId() + ",user_id:" + user_id + ",spot_id:" + spot_id + "}";
	}

	public static UserSpot fromDB( SimpleRowSet rs ) throws Exception{
		if ( rs != null )
		{
			UserSpot o = new UserSpot();
			o.setId( rs.getLong("id") );
			o.setUser_id( rs.getLong("user_id") );
			o.setSpot_id( rs.getLong("spot_id") );
			o.setUser_checkins_count( rs.getInt("user_checkins_count") );
			return o;
		}
		return null;
	}

	public static UserSpot fromVisitedSpots(long userId, String rs) throws Exception{
		if ( rs != null )
		{
			UserSpot o = new UserSpot();
			o.setUser_id( userId );
			String spotId = rs.replace("/spots/", "");
			o.setSpot_id( Long.valueOf(spotId) );
			o.setUser_checkins_count(1);
			return o;
		}
		return null;
	}

	public static UserSpot fromUserTopSpots(long userId, JSONObject rs) throws Exception{
		if ( rs != null )
		{
			UserSpot o = new UserSpot();
			o.setUser_id( userId );
			String spotId = rs.getString("url").replace("/spots/", "");
			o.setSpot_id( Long.valueOf(spotId) );
			o.setUser_checkins_count( rs.getInt("user_checkins_count") );
			return o;
		}
		return null;
	}

	public static UserSpot fromSpotTopTen(long spotId, JSONObject rs) throws Exception{
		if ( rs != null )
		{
			UserSpot o = new UserSpot();
			o.setSpot_id( spotId );
			String userId = rs.getString("url").replace("/users/", "");
			o.setUser_id( Long.valueOf(userId) );
			o.setUser_checkins_count( rs.getInt("checkins_count") );
			return o;
		}
		return null;
	}

	public String getTableName(){
		return getTableName(this.getUser_id(), TABLE, NO_OF_TABLES);
	}
	public static String getTableName(long id){
		return getTableName(id, TABLE, NO_OF_TABLES);
	}
}
