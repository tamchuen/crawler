package com.tamchuen.crawler.brightkite.domain;

import org.json.JSONObject;

import com.tamchuen.jutil.sql.SimpleRowSet;

public class UserFriend extends IntegerIDObject{
    public static final String TABLE = "userFriends";
    public static final int NO_OF_TABLES = 1;

    private String user1;
    private String user2;
    
    
    
    public String getUser1() {
		return user1;
	}

	public void setUser1(String user1) {
		this.user1 = user1;
	}

	public String getUser2() {
		return user2;
	}

	public void setUser2(String user2) {
		this.user2 = user2;
	}

	public String toString(){
	return "{id=" + this.getId() + ",user1:" + user1 + ",user2:" + user2 + "}" ;
    }
    
    public static UserFriend fromDB( SimpleRowSet rs ) throws Exception{
	if ( rs != null )
	{
	    UserFriend o = new UserFriend();
	    o.setId( rs.getLong("id") );
	    o.setUser1( rs.getString("user1") );
	    o.setUser2( rs.getString("user2") );
	    return o;
	}
	return null;
    }
    
    public static UserFriend fromJSON(String userId, JSONObject rs) throws Exception{
	if ( rs != null )
	{
	    UserFriend o = new UserFriend();
	    o.setUser1( userId );
	    o.setUser2( rs.getString("id" ) );
	    return o;
	}
	return null;
    }
    
    public String getTableName(){
    	return getTableName(this.getUser1());
    }
    public static String getTableName(String id){
    	return  TABLE;
    }
}
