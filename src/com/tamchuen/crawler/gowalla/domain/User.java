package com.tamchuen.crawler.gowalla.domain;

import org.json.JSONObject;

import com.tamchuen.jutil.sql.SimpleRowSet;
import com.tamchuen.jutil.string.StringUtil;

/**
 * User
 */
public class User extends GowallaObject{
    public static final String TABLE = "user";
    public static final int NO_OF_TABLES = 1;

    private String first_name;

    private String last_name;

    private String hometown;

    private int challenge_pin_count;

    private int trip_pin_count;

    private int state_pin_count;

    private int following_count;

    private int province_pin_count;

    private int stamps_count;

    private int pins_count;

    private int country_pin_count;

    private int region_pin_count;

    private int highlights_count;

    private int trips_count;

    private int items_count;

    private int blocking_count;

    private int followers_count;

    private int friends_count;

    private int photos_count;

    private long facebook_id;

    private long twitter_id;

    private String twitter_username;

    
    
    public String getFirst_name() {
		return first_name;
	}

	public void setFirst_name(String first_name) {
		this.first_name = first_name;
	}

	public String getLast_name() {
		return last_name;
	}

	public void setLast_name(String last_name) {
		this.last_name = last_name;
	}

	public String getHometown() {
		return hometown;
	}

	public void setHometown(String hometown) {
		this.hometown = hometown;
	}

	public int getChallenge_pin_count() {
		return challenge_pin_count;
	}

	public void setChallenge_pin_count(int challenge_pin_count) {
		this.challenge_pin_count = challenge_pin_count;
	}

	public int getTrip_pin_count() {
		return trip_pin_count;
	}

	public void setTrip_pin_count(int trip_pin_count) {
		this.trip_pin_count = trip_pin_count;
	}

	public int getState_pin_count() {
		return state_pin_count;
	}

	public void setState_pin_count(int state_pin_count) {
		this.state_pin_count = state_pin_count;
	}

	public int getFollowing_count() {
		return following_count;
	}

	public void setFollowing_count(int following_count) {
		this.following_count = following_count;
	}

	public int getProvince_pin_count() {
		return province_pin_count;
	}

	public void setProvince_pin_count(int province_pin_count) {
		this.province_pin_count = province_pin_count;
	}

	public int getStamps_count() {
		return stamps_count;
	}

	public void setStamps_count(int stamps_count) {
		this.stamps_count = stamps_count;
	}

	public int getPins_count() {
		return pins_count;
	}

	public void setPins_count(int pins_count) {
		this.pins_count = pins_count;
	}

	public int getCountry_pin_count() {
		return country_pin_count;
	}

	public void setCountry_pin_count(int country_pin_count) {
		this.country_pin_count = country_pin_count;
	}

	public int getRegion_pin_count() {
		return region_pin_count;
	}

	public void setRegion_pin_count(int region_pin_count) {
		this.region_pin_count = region_pin_count;
	}

	public int getHighlights_count() {
		return highlights_count;
	}

	public void setHighlights_count(int highlights_count) {
		this.highlights_count = highlights_count;
	}

	public int getTrips_count() {
		return trips_count;
	}

	public void setTrips_count(int trips_count) {
		this.trips_count = trips_count;
	}

	public int getItems_count() {
		return items_count;
	}

	public void setItems_count(int items_count) {
		this.items_count = items_count;
	}

	public int getBlocking_count() {
		return blocking_count;
	}

	public void setBlocking_count(int blocking_count) {
		this.blocking_count = blocking_count;
	}

	public int getFollowers_count() {
		return followers_count;
	}

	public void setFollowers_count(int followers_count) {
		this.followers_count = followers_count;
	}

	public int getFriends_count() {
		return friends_count;
	}

	public void setFriends_count(int friends_count) {
		this.friends_count = friends_count;
	}

	public int getPhotos_count() {
		return photos_count;
	}

	public void setPhotos_count(int photos_count) {
		this.photos_count = photos_count;
	}

	public long getFacebook_id() {
		return facebook_id;
	}

	public void setFacebook_id(long facebook_id) {
		this.facebook_id = facebook_id;
	}

	public long getTwitter_id() {
		return twitter_id;
	}

	public void setTwitter_id(long twitter_id) {
		this.twitter_id = twitter_id;
	}

	public String getTwitter_username() {
		return twitter_username;
	}

	public void setTwitter_username(String twitter_username) {
		this.twitter_username = twitter_username;
	}

	public String toString(){
	return "{id=" + this.getId() + ",name:" + first_name + " " + last_name + "}" ;
    }
    
    public static User fromDB( SimpleRowSet rs ) throws Exception{
	if ( rs != null )
	{
	    User o = new User();
	    o.setId( rs.getLong("id") );
	    o.setFirst_name( rs.getString("first_name") );
	    o.setLast_name( rs.getString("last_name") );
	    o.setHometown( rs.getString("hometown") );
	    o.setChallenge_pin_count( rs.getInt("challenge_pin_count") );
	    o.setTrip_pin_count( rs.getInt("trip_pin_count") );
	    o.setState_pin_count( rs.getInt("state_pin_count") );
	    o.setFollowing_count( rs.getInt("following_count") );
	    o.setProvince_pin_count( rs.getInt("province_pin_count") );
	    o.setStamps_count( rs.getInt("stamps_count") );
	    o.setPins_count( rs.getInt("pins_count") );
	    o.setCountry_pin_count( rs.getInt("country_pin_count") );
	    o.setRegion_pin_count( rs.getInt("region_pin_count") );
	    o.setHighlights_count( rs.getInt("highlights_count") );
	    o.setTrips_count( rs.getInt("trips_count") );
	    o.setItems_count( rs.getInt("items_count") );
	    o.setBlocking_count( rs.getInt("blocking_count") );
	    o.setFollowers_count( rs.getInt("followers_count") );
	    o.setFriends_count( rs.getInt("friends_count") );
	    o.setPhotos_count( rs.getInt("photos_count") );
	    o.setFacebook_id( rs.getLong("facebook_id") );
	    o.setTwitter_id( rs.getLong("twitter_id") );
	    o.setTwitter_username( rs.getString("twitter_username") );
	    return o;
	}
	return null;
    }
    
    public static User fromJSON(long userId, JSONObject rs) throws Exception{
	if ( rs != null )
	{
	    User o = new User();
	    o.setId( userId );
	    // first need to extract the English characters,digit, underscore
	    o.setFirst_name( StringUtil.encodeSQL( rs.getString("first_name") ) );
	    o.setLast_name( StringUtil.encodeSQL( rs.getString("last_name") ) );
	    o.setHometown( StringUtil.encodeSQL( rs.getString("hometown")) );
	    
	    o.setChallenge_pin_count( rs.getInt("challenge_pin_count") );
	    o.setTrip_pin_count( rs.getInt("trip_pin_count") );
	    o.setState_pin_count( rs.getInt("state_pin_count") );
	    o.setFollowing_count( rs.getInt("following_count") );
	    o.setProvince_pin_count( rs.getInt("province_pin_count") );
	    o.setStamps_count( rs.getInt("stamps_count") );
	    o.setPins_count( rs.getInt("pins_count") );
	    o.setCountry_pin_count( rs.getInt("country_pin_count") );
	    o.setRegion_pin_count( rs.getInt("region_pin_count") );
	    o.setHighlights_count( rs.getInt("highlights_count") );
	    o.setTrips_count( rs.getInt("trips_count") );
	    o.setItems_count( rs.getInt("items_count") );
	    o.setBlocking_count( rs.getInt("blocking_count") );
	    o.setFollowers_count( rs.getInt("followers_count") );
	    o.setFriends_count( rs.getInt("friends_count") );
	    o.setPhotos_count( rs.getInt("photos_count") );
	    
	    // two situations:  facebook_id:1234 or facebook_id:null
	    // if it's null then rs.getLong("facebook_id") will throw exception, so need to check
	    if(! rs.isNull( "facebook_id")){
		o.setFacebook_id( rs.getLong("facebook_id") );
	    }
	    if(! rs.isNull( "twitter_id")){
		o.setTwitter_id( rs.getLong("twitter_id") );
	    }
	    // two situations:  twitter_username:"user123" or twitter_username:null
	    // if it's null then rs.getString("twitter_username") will return "null" 
	    if(! rs.isNull( "twitter_username")){
		o.setTwitter_username( StringUtil.encodeSQL( rs.getString("twitter_username")) );
	    }
	    
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
