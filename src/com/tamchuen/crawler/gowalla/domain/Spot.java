package com.tamchuen.crawler.gowalla.domain;

import org.json.JSONArray;
import org.json.JSONObject;

import com.tamchuen.jutil.sql.SimpleRowSet;
import com.tamchuen.jutil.string.StringUtil;

public class Spot extends GowallaObject{
	public static final String TABLE = "spot";
	public static final int NO_OF_TABLES = 2;

	private String name;
	private int checkins_count;
	private String created_at;
	private int max_items_count;
	private int trending_level;
	private long creator_id;
	private int users_count;
	private double lat;
	private double lng;
	private int highlights_count;
	private int photos_count1;
	private int photos_count2;
	private int items_count;
	private String description;
	private String foursquare_id;
	private int radius_meters;
	private int strict_radius;
	// added 2011 11/16
	private String founders;
	private String spot_categories;
	private String yelp_url;
	private String street_address;
	private String city;
	private String region;
	private String country;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getCheckins_count() {
		return checkins_count;
	}

	public void setCheckins_count(int checkins_count) {
		this.checkins_count = checkins_count;
	}

	public String getCreated_at() {
		return created_at;
	}

	public void setCreated_at(String created_at) {
		this.created_at = created_at;
	}

	public int getMax_items_count() {
		return max_items_count;
	}

	public void setMax_items_count(int max_items_count) {
		this.max_items_count = max_items_count;
	}

	public int getTrending_level() {
		return trending_level;
	}

	public void setTrending_level(int trending_level) {
		this.trending_level = trending_level;
	}

	public long getCreator_id() {
		return creator_id;
	}

	public void setCreator_id(long creator_id) {
		this.creator_id = creator_id;
	}

	public int getUsers_count() {
		return users_count;
	}

	public void setUsers_count(int Spots_count) {
		this.users_count = Spots_count;
	}

	public double getLat() {
		return lat;
	}

	public void setLat(double d) {
		this.lat = d;
	}

	public double getLng() {
		return lng;
	}

	public void setLng(double lng) {
		this.lng = lng;
	}

	public int getHighlights_count() {
		return highlights_count;
	}

	public void setHighlights_count(int highlights_count) {
		this.highlights_count = highlights_count;
	}

	public int getPhotos_count1() {
		return photos_count1;
	}

	public void setPhotos_count1(int photos_count1) {
		this.photos_count1 = photos_count1;
	}

	public int getPhotos_count2() {
		return photos_count2;
	}

	public void setPhotos_count2(int photos_count2) {
		this.photos_count2 = photos_count2;
	}

	public int getItems_count() {
		return items_count;
	}

	public void setItems_count(int items_count) {
		this.items_count = items_count;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getFoursquare_id() {
		return foursquare_id;
	}

	public void setFoursquare_id(String foursquare_id) {
		this.foursquare_id = foursquare_id;
	}

	public int getRadius_meters() {
		return radius_meters;
	}

	public void setRadius_meters(int radius_meters) {
		this.radius_meters = radius_meters;
	}

	public int getStrict_radius() {
		return strict_radius;
	}

	public void setStrict_radius(int strict_radius) {
		this.strict_radius = strict_radius;
	}

	public String getFounders() {
		return founders;
	}

	public void setFounders(String founders) {
		this.founders = founders;
	}

	public String getSpot_categories() {
		return spot_categories;
	}

	public void setSpot_categories(String spot_categories) {
		this.spot_categories = spot_categories;
	}

	public String getYelp_url() {
		return yelp_url;
	}

	public void setYelp_url(String yelp_url) {
		this.yelp_url = yelp_url;
	}

	public String getStreet_address() {
		return street_address;
	}

	public void setStreet_address(String street_address) {
		this.street_address = street_address;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getRegion() {
		return region;
	}

	public void setRegion(String region) {
		this.region = region;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String toString(){
		return "{id=" + this.getId() + ",name:" + name + ",lat:" + lat +",lng:" + lng + "}";
	}

	public static Spot fromDB( SimpleRowSet rs ) throws Exception{
		if ( rs != null )
		{
			Spot o = new Spot();
			o.setId( rs.getLong("id") );
			o.setName( rs.getString("name") );
			o.setCheckins_count( rs.getInt("checkins_count") );
			o.setCreated_at( rs.getString("created_at") );
			o.setMax_items_count( rs.getInt("max_items_count") );
			o.setTrending_level( rs.getInt("trending_level") );
			o.setCreator_id( rs.getLong("creator_id") );
			o.setUsers_count( rs.getInt("users_count") );
			o.setLat( rs.getDouble("lat") );
			o.setLng( rs.getDouble("lng") );
			o.setHighlights_count( rs.getInt("highlights_count") );
			o.setPhotos_count1( rs.getInt("photos_count1") );
			o.setPhotos_count2( rs.getInt("photos_count2") );
			o.setItems_count( rs.getInt("items_count") );
			o.setDescription( rs.getString("description") );
			o.setFoursquare_id( rs.getString("foursquare_id") );
			o.setRadius_meters( rs.getInt("radius_meters") );
			o.setStrict_radius( rs.getInt("strict_radius") );
			o.setFounders( rs.getString("founders") );
			o.setSpot_categories( rs.getString("spot_categories") );
			o.setYelp_url( rs.getString("yelp_url") );
			o.setStreet_address( rs.getString("street_address") );
			o.setCity( rs.getString("city") );
			o.setRegion( rs.getString("region") );
			o.setCountry( rs.getString("country") );
			return o;
		}
		return null;
	}

	public static Spot fromJSON(long spotId, JSONObject rs) throws Exception{
		if ( rs != null )
		{
			Spot o = new Spot();
			o.setId( spotId );
			// first need to extract the English characters,digit, underscore
			String name = rs.getString("name");
			o.setName( StringUtil.encodeSQL( name ) );

			o.setCheckins_count( rs.getInt("checkins_count") );
			o.setCreated_at( rs.getString("created_at") );
			o.setMax_items_count( rs.getInt("max_items_count") );
			o.setTrending_level( rs.getInt("trending_level") );
			// get creator id from json
			JSONObject creator = rs.getJSONObject("creator");
			String creatorId = getIdFromJsonObject(creator, "url","/users/" );
			o.setCreator_id( StringUtil.convertLong(creatorId, 0) );

			o.setUsers_count( rs.getInt("users_count") );
			o.setLat( Double.valueOf( rs.getString("lat") ) );
			o.setLng( Double.valueOf( rs.getString("lng") ) );
			o.setHighlights_count( rs.getInt("highlights_count") );
			o.setPhotos_count1( rs.getInt("_photos_count") );
			o.setPhotos_count2( rs.getInt("photos_count") );
			o.setItems_count( rs.getInt("items_count") );
			// will have problem for '\u2028' unicode character (line seperator)
			// 2011 11/16: use utf-8/unicode for DB
			o.setDescription( StringUtil.encodeSQL( StringUtil.abbreviate(rs.getString("description"),500 )) );

			if( ! rs.isNull("foursquare_id")){
				o.setFoursquare_id( rs.getString("foursquare_id") );
			}

			o.setRadius_meters( rs.getInt("radius_meters") );
			// need to convert boolean to tinyint
			o.setStrict_radius( rs.getBoolean("strict_radius") ? 1 :0 );

			o.setFounders( getIdsFromJsonArray(rs, "founders", "url", "/users/") );
			o.setSpot_categories( getIdsFromJsonArray(rs, "spot_categories", "url", "/categories/") );

			o.setYelp_url( rs.getString("yelp_url") );

			// get address from json
			JSONObject address = rs.getJSONObject("address");
			o.setStreet_address( address.getString("street_address") );
			o.setCity( address.getString("locality") );
			o.setRegion( address.getString("region") );
			o.setCountry( address.getString("iso3166") );

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
