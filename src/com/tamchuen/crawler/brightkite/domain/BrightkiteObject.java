package com.tamchuen.crawler.brightkite.domain;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.tamchuen.jutil.string.StringUtil;

/**
 * abstract class for all objects that can be stored into database table
 * @author  Dequan
 */
abstract class BrightkiteObject {

	private String id;

	public BrightkiteObject(){
		this.id = "";
	}
	public String getId() {
		return id;
	}
	/**
	 * @param id
	 * 
	 */
	public void setId(String id) {
		this.id = id;
	}
	/**
	 * get the table name for storing this object
	 * @return
	 * 
	 */
	abstract public String getTableName();
	/**
	 * util function to get a table name according to an id
	 * @param id
	 * @param table
	 * @param noOfTables
	 * @return
	 */
	public static String getTableName(String id, String table, int noOfTables){
		return table;
		//if( noOfTables <=1 ){
		//	return table;
		//}

		//int tableIdx = (int) id % noOfTables + 1;
		//return table + tableIdx;
	}
	/**
	 * get formated string for SQL, ensure that is only contains English characters, or digits, or underscore
	 * 2011-11/16  no need ! solve it in database. It can accept unicode string
	 * @param value
	 * @return
	 */
	public static String formatString(String value){
		if( value == null ){
			return "";
		}
		return value;
		//return StringUtil.encodeSQL( value.replaceAll("[^\\w+\\s$]","") );
	}

	/**
	 * json is like:
	 * "founders": [
	 *   {
	 *    "url": "/users/156",
	 *    "last_name": "Freund",
	 *     "first_name": "Phil"
	 *	}
	 *] or "founders": []
	 *	We need to get the id like 156 from it
	 * @return a string representing IDs like 156,145
	 */
	public static String getIdsFromJsonArray(JSONObject rs, String arrayNodeName, String valueNodeName, String strToReplace) throws JSONException{
		String result = "";

		JSONArray items = rs.getJSONArray(arrayNodeName);
		for(int i=0; i < items.length(); i ++ ){
			JSONObject obj = items.getJSONObject(i);

			result += getIdFromJsonObject(obj,valueNodeName, strToReplace );

			if( i != items.length() -1){
				result += ",";
			}
		}

		return result;
	}
	/**
	 * get id from :
	 *  "creator": {
	 *    "url": "/users/1",
	 *    "last_name": "Raymond",
	 *    "first_name": "Scott"
	 *  }
	 * @param rs
	 * @param valueNodeName
	 * @param strToReplace
	 * @return
	 * @throws JSONException
	 */
	public static String getIdFromJsonObject(JSONObject rs, String valueNodeName, String strToReplace) throws JSONException{
		String szId = "";
		if( rs.has(valueNodeName)){
			szId = rs.getString(valueNodeName); 
		}

		if(strToReplace!= null ){
			szId = szId.replace(strToReplace, "");
		}

		return szId;
	}
}
