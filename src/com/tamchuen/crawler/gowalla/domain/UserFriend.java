package com.tamchuen.crawler.gowalla.domain;

import org.json.JSONObject;

import com.tamchuen.jutil.sql.SimpleRowSet;

public class UserFriend extends GowallaObject{
    public static final String TABLE = "userFriends";
    public static final int NO_OF_TABLES = 5;

    private long user1;

    private long user2;

    public long getUser1() {
        return user1;
    }
    public void setUser1(long user1) {
        this.user1 = user1;
    }
    public long getUser2() {
        return user2;
    }
    public void setUser2(long user2) {
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
	    o.setUser1( rs.getLong("user1") );
	    o.setUser2( rs.getLong("user2") );
	    return o;
	}
	return null;
    }
    
    public static UserFriend fromJSON(long userId, JSONObject rs) throws Exception{
	if ( rs != null )
	{
	    UserFriend o = new UserFriend();
	    o.setUser1( userId );
	    String userId2 = rs.getString("url" ).replace("/users/", "");
	    o.setUser2( Long.valueOf(userId2) );
	    return o;
	}
	return null;
    }
    
    public String getTableName(){
	return getTableName(this.getUser1(), TABLE, NO_OF_TABLES);
    }
    public static String getTableName(long id){
	return getTableName(id, TABLE, NO_OF_TABLES);
    }
}
