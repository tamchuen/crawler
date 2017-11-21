package com.tamchuen.crawler.brightkite.domain;

import org.json.JSONObject;

import com.tamchuen.jutil.sql.SimpleRowSet;
import com.tamchuen.jutil.string.StringUtil;

public class UserCheckin extends BrightkiteObject{
	public static final String TABLE = "userCheckins";
	public static final int NO_OF_TABLES = 1;
	private String user_id;
	private String spot_id;
	private String created_at; 
	private String via;
	private int rating;
	private int ratings_count;
	private int comments_count;
	private int view_count;
	
	public String getCreated_at() {
		return created_at;
	}

	public void setCreated_at(String created_at) {
		this.created_at = created_at;
	}

	public String getUser_id() {
		return user_id;
	}

	public void setUser_id(String user_id) {
		this.user_id = user_id;
	}

	public String getSpot_id() {
		return spot_id;
	}

	public void setSpot_id(String spot_id) {
		this.spot_id = spot_id;
	}

	public String getVia() {
		return via;
	}

	public void setVia(String via) {
		this.via = via;
	}

	public int getRating() {
		return rating;
	}

	public void setRating(int rating) {
		this.rating = rating;
	}

	public int getRatings_count() {
		return ratings_count;
	}

	public void setRatings_count(int ratings_count) {
		this.ratings_count = ratings_count;
	}

	public int getComments_count() {
		return comments_count;
	}

	public void setComments_count(int comments_count) {
		this.comments_count = comments_count;
	}

	public int getView_count() {
		return view_count;
	}

	public void setView_count(int view_count) {
		this.view_count = view_count;
	}

	public String toString(){
		return "{id=" + this.getId() + ",user_id:" + user_id + ",spot_id:" + spot_id  + "}";
	}

	public static UserCheckin fromDB( SimpleRowSet rs ) throws Exception{
		if ( rs != null )
		{
			UserCheckin o = new UserCheckin();
			o.setId( rs.getString("id") );
			o.setUser_id( rs.getString("user_id") );
			o.setSpot_id( rs.getString("spot_id") );
			o.setCreated_at( rs.getString("created_at") );
			o.setVia( rs.getString("via") );
			o.setRating( rs.getInt("rating") );
			o.setRatings_count( rs.getInt("ratings_count") );
			o.setComments_count( rs.getInt("comments_count") );
			o.setView_count( rs.getInt("view_count") );
			return o;
		}
		return null;
	}

	public static UserCheckin fromJSON( JSONObject rs) throws Exception{
		if ( rs != null )
		{
			UserCheckin o = new UserCheckin();
			o.setId( rs.getString("id") );
			
			// get user id 
			JSONObject userObj = rs.getJSONObject("creator");
			if( userObj != null){
				o.setUser_id( userObj.getString("id") );
			}
			// get spot id 
			JSONObject spotObj = rs.getJSONObject("place");
			if( userObj != null){
				o.setSpot_id( spotObj.getString("id") );
			}
			o.setCreated_at( rs.getString("created_at") );
			o.setVia( rs.getString("via") );
			o.setRating( rs.getInt("rating") );
			o.setRatings_count( rs.getInt("ratings_count") );
			o.setComments_count( rs.getInt("comments_count") );
			o.setView_count( rs.getInt("view_count") );
			return o;
		}
		return null;
	}

	public String getTableName(){
		return getTableName(this.getCreated_at());
	}
	
	public static String getTableName(String created_at){
		String table = TABLE;
		if( created_at == null || created_at.length() < 6 ){
			return TABLE;
		}
		
		// use the date to distribute the records
		String parts[] = StringUtil.split(created_at, "/");
		if( parts == null || parts.length < 2){
			return TABLE;
		}		
		
		String year = parts[0];
		int month = StringUtil.convertInt(parts[1], 0 );
		// month 1-6 will be 1 table, and month 7-12 will be another table
		if( month <= 6 && month >= 1){
			table = TABLE + "_" + year + "_1_6";
		}else if( month <= 12 && month >= 7){
			table = TABLE + "_" + year + "_7_12";
		}
		
		return table;
	}
	
	public static void main(String args[]){
		System.out.println( UserCheckin.getTableName("2008/01"));
		System.out.println( UserCheckin.getTableName("2010/9"));
		System.out.println( UserCheckin.getTableName("2011/12"));
	}
}
