package com.tamchuen.crawler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.tamchuen.jutil.string.StringUtil;
import com.tamchuen.jutil.util.FileUtil;

/**
 * Pre Process the gowalla result 
 * @author  Dequan
 * Project: Crawler
 * Date:    Dec 14, 2011
 * 
 */
public class GowallProcessor {
	public final static String userSpotsFile = "E:\\My Documents\\Study\\Recommender\\Gowalla\\user-spots-1128.csv";
	public final static String userSpotsCheckins = "E:\\My Documents\\Study\\Recommender\\Gowalla\\user-spots-checkins.csv";
	public final static String spotUsersCheckins = "E:\\My Documents\\Study\\Recommender\\Gowalla\\spot-users-checkins.csv";
	public final static String tfidfResultFile = "E:\\My Documents\\Study\\Recommender\\Gowalla\\tfidf-result.csv";
	
	public static void main(String args[]){
		// preprocess user/spot/checkins
		processUserSpotCheckinsCount(userSpotsFile, true);
		
		// preprocess user/spot and create TFIDF value
		
	}
	
	/**
	 * create the [userid, # locations, # checkins by user] and <br/>
	 *            [spotid, # users, # checkins in spot]
	 * @param relFile
	 * @param containsHeader
	 */
	public static void  processUserSpotCheckinsCount( String relFile, boolean containsHeader ){
		List<String> relList = FileUtil.readFile2List(relFile);
		if( relList == null || relList.size() == 0 ){
			return;
		}
		
		int startIdx = 0;
		if( containsHeader ){
			startIdx = 1;
		}
		
	    List<Preference> allUserSpots = new ArrayList<Preference>();
	    Map<String, List<Preference>> allUsers = new HashMap<String,List<Preference>>();
	    Map<String, List<Preference>> allSpots = new HashMap<String,List<Preference>>();
	    
	    Map<String,CheckinInfo> userCheckinInfo = new HashMap<String,CheckinInfo>();
	    Map<String,CheckinInfo> spotCheckinInfo = new HashMap<String,CheckinInfo>();
	    
	    for(int i = startIdx; i < relList.size(); i ++ ){
			String line = relList.get(i);
			String[] lineNodes = line.split(",");
			if( lineNodes.length < 3){
				break;
			}

			String userId = lineNodes[0];
			String itemId = lineNodes[1];
			double iCheckinCount = StringUtil.convertDouble(lineNodes[2], 0 );
			Preference uc = new Preference();
			
			List<Preference> list1 = allUsers.get(userId);
			if( list1 == null ){
				list1 = new ArrayList<Preference>();
				allUsers.put(userId, list1);
			}
			List<Preference> list2 = allSpots.get(itemId);
			if( list2 == null ){
				list2 = new ArrayList<Preference>();
				allSpots.put(itemId, list2);
			}
			
			uc.userId = userId;
			uc.itemId = itemId;
			uc.value = iCheckinCount;
			
			allUserSpots.add(uc);
			list1.add(uc);
			list2.add(uc);
		}
	    
	    log("Total userSpots: " + allUserSpots.size() + ", users " + allUsers.size() + ", spots " + allSpots.size() );
	    StringBuilder sb1 = new StringBuilder("User, # locations, # checkins \n");
	    for( Iterator<Map.Entry<String,List<Preference>>> iter = allUsers.entrySet().iterator(); iter.hasNext(); ){
	    	Map.Entry<String,List<Preference>> entry = iter.next();
	    	String userId = entry.getKey();
	    	List<Preference> userSpots = entry.getValue();
	    	int iCheckins = 0;
	    	for( Preference u: userSpots ){
	    		iCheckins += u.value;
	    	}
	    	sb1.append( userId + "," + userSpots.size() + "," + iCheckins +"\n");
	    	// save 
	    	CheckinInfo checkinInfo = new CheckinInfo();
	    	checkinInfo.id = userId;
	    	checkinInfo.value1 =  userSpots.size();
	    	checkinInfo.value2 = iCheckins;
	    	userCheckinInfo.put(userId, checkinInfo);
	    }
	    
	    StringBuilder sb2 = new StringBuilder("Spot, # users, # checkins \n");
	    for( Iterator<Map.Entry<String,List<Preference>>> iter = allSpots.entrySet().iterator(); iter.hasNext(); ){
	    	Map.Entry<String,List<Preference>> entry = iter.next();
	    	String spotId = entry.getKey();
	    	List<Preference> userSpots = entry.getValue();
	    	int iCheckins = 0;
	    	for( Preference u: userSpots ){
	    		iCheckins += u.value;
	    	}
	    	sb2.append( spotId + "," + userSpots.size() + "," + iCheckins+"\n" );
	    	// save
	    	CheckinInfo checkinInfo = new CheckinInfo();
	    	checkinInfo.id = spotId;
	    	checkinInfo.value1 =  userSpots.size();
	    	checkinInfo.value2 = iCheckins;
	    	spotCheckinInfo.put(spotId, checkinInfo);
	    }
	    
	    // save the checkin info result to files
	    //FileUtil.writeFile(userSpotsCheckins, sb1.toString());
	    //FileUtil.writeFile(spotUsersCheckins, sb2.toString());
	    
	    // process the TFIDF scores
	    log( "TFIDF");
	    StringBuilder sb3 = new StringBuilder( "userId, spotId, tf_u, idf_u, tfidf_u, tf_l, idf_l, tfidf_l, tfidf\n");
	    for(Preference pf : allUserSpots){
	    	// calculate TFIDF_U
	    	double tf_u = 0.0;
	    	double idf_u = 0.0;
	    	double tfidf_u = 0.0;
	    	// # of checkins by u in l
	    	double iCheckinByUinL = pf.value;
	    	// # of checkins in l by all users
	    	double iCheckinInL = spotCheckinInfo.get(  pf.itemId ).value2;
	    	// total # of locations
	    	double iTotalLocations = allSpots.size();
	    	// # of locations where u has checkins 
	    	double iLocationsByU = userCheckinInfo.get( pf.userId ).value1; 
	    	
	    	if( iCheckinInL >0 ){
	    		tf_u = iCheckinByUinL/iCheckinInL;
	    	}
	    	if( iLocationsByU > 0 ){
	    		idf_u = Math.log( iTotalLocations/iLocationsByU );
	    	}
	    	
	    	tfidf_u = tf_u * idf_u;
	    	
	    	// calculate TFIDF_L
	    	double tf_l = 0.0;
	    	double idf_l = 0.0;
	    	double tfidf_l = 0.0;
	    	
	    	// # of checkins by u in all locations
	    	double iCheckinByU = userCheckinInfo.get(  pf.userId ).value2;
	    	// total # of users
	    	double iTotalUsers = allUsers.size();
	    	// # of users who have checkin in L 
	    	double iUsersInL = spotCheckinInfo.get( pf.itemId ).value1; 
	    	
	    	if( iCheckinByU >0 ){
	    		tf_l = iCheckinByUinL/iCheckinByU;
	    	}
	    	if( iUsersInL > 0 ){
	    		idf_l = Math.log( iTotalUsers/iUsersInL );
	    	}
	    	
	    	tfidf_l = tf_l * idf_l;

	    	double tfidf = tfidf_u + tfidf_l;
	    	//log( pf.userId + "," + pf.itemId + "," + tf_u + "," + idf_u + "," + tfidf_u+ "," + tf_l + "," + idf_l + "," + tfidf_l + "," + tfidf);
	    	sb3.append(pf.userId + "," + pf.itemId + "," + tf_u + "," + idf_u + "," + tfidf_u+ "," + tf_l + "," + idf_l + "," + tfidf_l + "," + tfidf + "\n");
	    }
	    // save the TFIDF result to files
	    FileUtil.writeFile(tfidfResultFile, sb3.toString());
	    
	    log("Sucess");
	}
	
	
	static class Preference{
		String userId;
		String itemId;
		double value;
	}
	
	static class CheckinInfo{
		/**
		 * userId/spotId
		 */
		String id; 
		/**
		 * # locations for a user/ # users in a location
		 */
		double value1;
		/**
		 *  # checkins by a user/location
		 */
		double value2;
	}
	
	public static void log(String msg){
		System.out.println("" + msg);
	}
}
