package com.tamchuen.crawler.brightkite.domain;

import org.json.JSONArray;
import org.json.JSONObject;

import com.tamchuen.jutil.sql.SimpleRowSet;
import com.tamchuen.jutil.string.StringUtil;

/**
 * User
 */
public class User extends BrightkiteObject{
    public static final String TABLE = "user";
    public static final int NO_OF_TABLES = 1;
    
    private String fullname;
    private String login;
    private String description;
    private String tag_list;
    private String sex;
    private int age;
    private int photos_count;
    private int friends_count;
    private int notes_count;
    private int checkins_count;
    private int fans_count;
    private int comments_count;
    private String social_links;
    
    public static User fromDB( SimpleRowSet rs ) throws Exception{
    	if ( rs != null )
    	{
    		User o = new User();
    		o.setId( rs.getString("id") );
    		o.setFullname( rs.getString("fullname") );
    		o.setLogin( rs.getString("login") );
    		o.setDescription( rs.getString("description") );
    		o.setTag_list( rs.getString("tag_list") );
    		o.setSex( rs.getString("sex") );
    		o.setAge( rs.getInt("age") );
    		o.setPhotos_count( rs.getInt("photos_count") );
    		o.setFriends_count( rs.getInt("friends_count") );
    		o.setNotes_count( rs.getInt("notes_count") );
    		o.setCheckins_count( rs.getInt("checkins_count") );
    		o.setFans_count( rs.getInt("fans_count") );
    		o.setComments_count( rs.getInt("comments_count") );
    		o.setSocial_links( rs.getString("social_links") );
    		return o;
    	}
    	return null;
    }

    public static User fromJSON(String userId, JSONObject rs) throws Exception{
    	if ( rs != null )
    	{
    		User o = new User();
    		o.setId( userId );
    		// first need to extract the English characters,digit, underscore

    		o.setFullname( StringUtil.encodeSQL(rs.getString("fullname")) );
    		o.setLogin( rs.getString("login") );
    		o.setDescription( StringUtil.encodeSQL( StringUtil.abbreviate( rs.getString("description"),256) ) );
    		
    		JSONArray tags = rs.getJSONArray("tag_list" );
    	    String tagList = "";
    		for(int i=0; i < tags.length(); i ++ ){
    	    	String tag = tags.getString(i);
    	    	// only need 256 length
    	    	if( tag.length() + tagList.length() > 256 ){
    	    		break;
    	    	}
    	    	tagList += StringUtil.encodeSQL( tag ) + ",";
    	    }	
    		o.setTag_list( tagList );
    		o.setSex( rs.getString("sex") );    		
    		o.setAge( StringUtil.convertInt(rs.getString("age"), 0) );
    		o.setPhotos_count( rs.getInt("photos_count") );
    		o.setFriends_count( rs.getInt("friends_count") );
    		o.setNotes_count( rs.getInt("notes_count") );
    		o.setCheckins_count( rs.getInt("checkins_count") );
    		o.setFans_count( rs.getInt("fans_count") );
    		o.setComments_count( rs.getInt("comments_count") );
    		
    		JSONArray items = rs.getJSONArray("social_links" );
    		String social_links = "";
    	    for(int i=0; i < items.length(); i ++ ){
    	    	JSONObject item = items.getJSONObject(i);
    	    	String serviceId = item.getString("service_id");
    	    	String serviceUserId = item.getString("user_id");
    	    	/*
    	    	if("digg".equals(serviceId) ){
    	    		o.setDigg_userid( serviceUserId );
    	    	}else if("twitter".equals(serviceId)){
    	    		o.setTwitter_userid( serviceUserId );
    	    	}else if("flickr".equals(serviceId)){
    	    		o.setFlickr_userid( serviceUserId );
    	    	}else if("facebook".equals(serviceId)){
    	    		int beginIndex = serviceUserId.lastIndexOf("/");
    	    		if( beginIndex > 0 ){
    	    			String facebookId = serviceUserId.substring(beginIndex + 1);
        	    		o.setFacebook_id( Long.valueOf( facebookId ) );
    	    		}	
    	    	}*/
    	    	if(serviceUserId.indexOf(",") >=0 || serviceUserId.indexOf(";")>=0 ){
    	    		continue;
    	    	}
    	    	// check length limit, only need 1024
    	    	String link = serviceId + "," + serviceUserId + ";";
    	    	if( social_links.length() + link.length() >= 1024){
    	    		break;
    	    	}
    	    	
    	    	social_links += link;
    	    }
    	    o.setSocial_links( social_links );
    		return o;
    	}
    	return null;
    }
    
	public String getFullname() {
		return fullname;
	}

	public void setFullname(String fullname) {
		this.fullname = fullname;
	}

	public String getLogin() {
		return login;
	}

	public void setLogin(String login) {
		this.login = login;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getTag_list() {
		return tag_list;
	}

	public void setTag_list(String tag_list) {
		this.tag_list = tag_list;
	}

	public String getSex() {
		return sex;
	}

	public void setSex(String sex) {
		this.sex = sex;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	public int getPhotos_count() {
		return photos_count;
	}

	public void setPhotos_count(int photos_count) {
		this.photos_count = photos_count;
	}

	public int getFriends_count() {
		return friends_count;
	}

	public void setFriends_count(int friends_count) {
		this.friends_count = friends_count;
	}

	public int getNotes_count() {
		return notes_count;
	}

	public void setNotes_count(int notes_count) {
		this.notes_count = notes_count;
	}

	public int getCheckins_count() {
		return checkins_count;
	}

	public void setCheckins_count(int checkins_count) {
		this.checkins_count = checkins_count;
	}

	public int getFans_count() {
		return fans_count;
	}

	public void setFans_count(int fans_count) {
		this.fans_count = fans_count;
	}

	public int getComments_count() {
		return comments_count;
	}

	public void setComments_count(int comments_count) {
		this.comments_count = comments_count;
	}

	public String getSocial_links() {
		return social_links;
	}

	public void setSocial_links(String twitter_userid) {
		this.social_links = twitter_userid;
	}
	
	public String toString(){
		return "{id=" + this.getId() + ",name:" + fullname + "}" ;
    }
    
    public String getTableName(){
    	return getTableName(this.getId(), TABLE, NO_OF_TABLES);
    }
    public static String getTableName(String id){
    	return getTableName(id, TABLE, NO_OF_TABLES);
    }
}
