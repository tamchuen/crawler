package com.tamchuen.crawler.brightkite.domain;

import org.json.JSONArray;
import org.json.JSONObject;

import com.tamchuen.jutil.sql.SimpleRowSet;
import com.tamchuen.jutil.string.StringUtil;

public class Spot extends BrightkiteObject{
	public static final String TABLE = "spot";
	public static final int NO_OF_TABLES = 1;
	private String name;
	private double latitude;
	private double longitude;
	private String attribution;
	private String scope;
	private String street;
	private String city;
	private String state;
	private String country;	
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public double getLatitude() {
		return latitude;
	}

	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}

	public double getLongitude() {
		return longitude;
	}

	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}

	public String getAttribution() {
		return attribution;
	}

	public void setAttribution(String attribution) {
		this.attribution = attribution;
	}

	public String getScope() {
		return scope;
	}

	public void setScope(String scope) {
		this.scope = scope;
	}

	public String getStreet() {
		return street;
	}

	public void setStreet(String street) {
		this.street = street;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String toString(){
		return "{id=" + this.getId() + ",name:" + name + ",lat:" + latitude +",lng:" + longitude + "}";
	}

	public static Spot fromDB( SimpleRowSet rs ) throws Exception{
		if ( rs != null )
		{
			Spot o = new Spot();
			o.setId( rs.getString("id") ); 
			o.setName( rs.getString("name") );
			o.setLatitude( rs.getDouble("latitude") );
			o.setLongitude( rs.getDouble("longitude") );
			o.setAttribution( rs.getString("attribution") );
			o.setScope( rs.getString("scope") );
			o.setStreet( rs.getString("street") );
			o.setCity( rs.getString("city") );
			o.setState( rs.getString("state") );
			o.setCountry( rs.getString("country") );
			return o;
		}
		return null;
	}

	public static Spot fromJSON(String spotId, JSONObject rs) throws Exception{
		if ( rs != null )
		{
			Spot o = new Spot();
			o.setId( spotId );
			o.setName( StringUtil.encodeSQL( rs.getString("name") ) );
			o.setLatitude( rs.getDouble("latitude") );
			o.setLongitude( rs.getDouble("longitude") );
			o.setAttribution( StringUtil.encodeSQL( StringUtil.abbreviate(rs.getString("attribution"),256 )) );
			o.setScope( rs.getString("scope") );
			o.setStreet( rs.getString("street") );
			o.setCity( rs.getString("city") );
			o.setState( rs.getString("state") );
			o.setCountry( rs.getString("country") );
			return o;
		}
		return null;
	}

	public String getTableName(){
		return getTableName(this.getId(), TABLE, NO_OF_TABLES);
	}
	public static String getTableName(String id){
		return getTableName(id, TABLE, NO_OF_TABLES);
	}
}
