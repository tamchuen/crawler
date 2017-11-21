package com.tamchuen.crawler.gowalla.domain;

import org.json.JSONObject;

import com.tamchuen.jutil.sql.SimpleRowSet;
import com.tamchuen.jutil.string.StringUtil;

public class UserCheckin extends GowallaObject{
	public static final String TABLE = "userCheckins";
	public static final int NO_OF_TABLES = 5;

	private long user_id;
	private long spot_id;
	private String created_at;
	private String message;
	private int photos_count;
	private int comments_count;

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
	public String getCreated_at() {
		return created_at;
	}
	public void setCreated_at(String created_at) {
		this.created_at = created_at;
	}
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	public int getPhotos_count() {
		return photos_count;
	}
	public void setPhotos_count(int photos_count) {
		this.photos_count = photos_count;
	}
	public int getComments_count() {
		return comments_count;
	}
	public void setComments_count(int comments_count) {
		this.comments_count = comments_count;
	}
	public String toString(){
		return "{id=" + this.getId() + ",user_id:" + user_id + ",spot_id:" + spot_id  + "}";
	}

	public static UserCheckin fromDB( SimpleRowSet rs ) throws Exception{
		if ( rs != null )
		{
			UserCheckin o = new UserCheckin();
			o.setId( rs.getLong("id") );
			o.setUser_id( rs.getLong("user_id") );
			o.setSpot_id( rs.getLong("spot_id") );
			o.setCreated_at( rs.getString("created_at") );
			o.setMessage( rs.getString("message") );
			o.setPhotos_count( rs.getInt("photos_count") );
			o.setComments_count( rs.getInt("comments_count") );
			return o;
		}
		return null;
	}

	public static UserCheckin fromCheckin(long checkinId, JSONObject rs) throws Exception{
		if ( rs != null )
		{
			UserCheckin o = new UserCheckin();
			o.setId( checkinId);
			// get user id 
			JSONObject userObj = rs.getJSONObject("user");
			if( userObj != null){
				String userId = userObj.getString("url" ).replace("/users/", "");
				o.setUser_id( Long.valueOf(userId) );
			}
			// get spot id 
			JSONObject spotObj = rs.getJSONObject("spot");
			if( userObj != null){
				String spotId = spotObj.getString("url" ).replace("/spots/", "");
				o.setSpot_id( Long.valueOf(spotId) );
			}

			o.setCreated_at( rs.getString("created_at") );
			o.setMessage( rs.getString("message") );
			return o;
		}
		return null;
	}

	public static UserCheckin fromSpotEvents(long spotId, JSONObject rs) throws Exception{
		if ( rs != null )
		{
			UserCheckin o = new UserCheckin();
			
			// only record type=checkin 
			String type = rs.getString("type");
			if( ! "checkin".equals(type)){
				return null;
			}
			
			String checkinId = rs.getString("url").replace("/checkins/", "");
			o.setId( Long.valueOf(checkinId) );
			// get user id 
			JSONObject userObj = rs.getJSONObject("user");
			String userId = getIdFromJsonObject(userObj, "url","/users/" );
			o.setUser_id( Long.valueOf(userId) );		
			// get spot id 
			o.setSpot_id( spotId );
			o.setCreated_at( rs.getString("created_at") );
			o.setMessage( StringUtil.encodeSQL( StringUtil.abbreviate(rs.getString("message"),250 )) );
			o.setPhotos_count( rs.getInt("photos_count") );
			o.setComments_count( rs.getInt("comments_count") );
			return o;
		}
		
		return null;
	}
	
	public String getTableName(){
		return getTableName(this.getId(), TABLE, NO_OF_TABLES);
	}
	public static String getTableName(long id){
		return getTableName(id, TABLE, NO_OF_TABLES);
	}
}
