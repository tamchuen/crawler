package com.tamchuen.crawler.util;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.tamchuen.jutil.j4log.Logger;
import com.tamchuen.jutil.sql.DBClientWrapper;
import com.tamchuen.jutil.sql.DBWrapperFactory;
import com.tamchuen.jutil.string.StringUtil;
import com.tamchuen.jutil.util.FileUtil;

/**
 * Process the text based checkin data from gowalla and brightkite
 * @author  Dequan
 * Project: Crawler
 * Date:    Apr 30, 2012
 * 
 */
public class ProcessTxtData {
	private static Logger systemLogger = Logger.getLogger("snap_data");
	private static DBClientWrapper DB_CONN1 = DBWrapperFactory.getDBClientWrapper("gowalla");
	private static DBClientWrapper DB_CONN2 = DBWrapperFactory.getDBClientWrapper("brightkite");
	private static String gowallaCheckinFile = "E:\\My Documents\\Study\\Recommender\\Summer2012\\Gowalla_totalCheckins.txt";
	private static String brightkiteCheckinFile = "E:\\My Documents\\Study\\Recommender\\Summer2012\\Brightkite_totalCheckins.txt";
	// user spots from 8AM to 8 PM
	private static String gowallaUserSpotsFile = "E:\\My Documents\\Study\\Recommender\\Summer2012\\Brightkite_userSpots2_8_20.txt";
	private static String brightkiteUserSpotsFile = "E:\\My Documents\\Study\\Recommender\\Summer2012\\Gowalla_userSpots2_8_20.txt";
	// change this 
	private static boolean isGowalla = true;
	
	public static void main(String args[]){
		//String inputFile = isGowalla ? gowallaCheckinFile : brightkiteCheckinFile;
		String inputFile = isGowalla ? gowallaUserSpotsFile : brightkiteUserSpotsFile;
		
		// Read file 
		List<String> relList = FileUtil.readFile2List(inputFile);
		if( relList == null || relList.size() == 0 ){
			System.err.println("File not exists!");
			System.exit(0);
		}
		
		System.out.println("Start for " + relList.size() + " records of " + (isGowalla ? "Gowalla" : "Brightkite") );
		
		long start = System.currentTimeMillis();
		
		//insertToDB(relList);
		DetectHomeOffice(relList);
		
		long end = System.currentTimeMillis();
		int elapsed = (int) (end-start)/1000;
		System.out.println("Finished for " + relList.size() + " records, time: " + elapsed + "s");
		
	}
	
	/**
	 * Detect Home/Office location, first convert the lat/lng into 25kmx25km grid,
	 * then find out the cell with the most #check-ins of each user, then find the average position inside that cell
	 * @param relList
	 */
	public static void DetectHomeOffice(List<String> relList){
		double theta = (25/6367.445) * (180/ Math.PI); 
		Map<Long, Map<String, CellData> > userCellsMap = new HashMap<Long, Map<String, CellData> >();
		Map<Long, String> userTopCellMap = new HashMap<Long, String>();
		
		// build the hashMap storing users and their corresponding list of cells
		System.out.println("===============================================\nBuilding cells..."  );
		for(int i = 0; i < relList.size(); i ++ ){
			String line = relList.get(i);
			String[] lineNodes = line.split(",");
			UserCheckin o = new UserCheckin();
			o.userId = StringUtil.convertLong(lineNodes[0], -1 );
			o.spotId = lineNodes[1];
			o.lat = StringUtil.convertDouble(lineNodes[2], 0 );
			o.lng = StringUtil.convertDouble(lineNodes[3], 0 );
			o.numCheckin = StringUtil.convertInt(lineNodes[4], 0 );

			if( i == 100 ){
				break;
			}

			try {
				// convert the lng,lat into X,Y
				// Xbin = [lng/theta] , Ybin = [lat/theta], should convert to integer
				int x_bin = (int) Math.floor( o.lng / theta );
				int y_bin = (int) Math.floor( o.lat / theta );
				String cellId =  x_bin + "|" + y_bin;
				System.out.println(o.userId + "," + cellId  );
				
				// add it into to hashMap
				Map<String, CellData> userCellList = userCellsMap.get(o.userId);
				// check if user exists
				if( userCellList == null  ){
					userCellList = new HashMap<String, CellData>();
					CellData cellData = new CellData();
					cellData.x = x_bin;
					cellData.y = y_bin;
					cellData.totalCheckin = o.numCheckin;
					cellData.latLngList.add( new LatLng(o.lat, o.lng ) );
					userCellList.put(cellId, cellData);
					// add the user and cellList to the map
					userCellsMap.put( o.userId, userCellList );
					System.out.println("add user:" + o.userId   );
				}else{
					// check if cell exist
					CellData cellData = userCellList.get(cellId);
					if( cellData == null ){
						// insert cellData for that user
						cellData = new CellData();
						cellData.x = x_bin;
						cellData.y = y_bin;
						cellData.totalCheckin = o.numCheckin;
						cellData.latLngList.add( new LatLng(o.lat, o.lng ) );
						userCellList.put(cellId, cellData);
						System.out.println("add cell:" + o.userId + "," + cellId  );
					}else{
						// cell exists, just update and add the latlng into the list
						cellData.totalCheckin += o.numCheckin;
						cellData.latLngList.add( new LatLng(o.lat, o.lng ) );
						System.out.println("add latlng:" + o.userId + "," + cellId + "," + o.lat + "," + o.lng );
					}	
				}
				
				//System.out.println(o.userId + "," + o.lng + "=>" + x_bin + "," + o.lat + "=>" + y_bin   );
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}// end for

		// get the cell with most num checkins for each user
		System.out.println("===============================================\nCalculating max cells..."  );
		Iterator<Long> userIdIter = userCellsMap.keySet().iterator();
		// loop for all users
		while( userIdIter.hasNext() ){
			Long userId = userIdIter.next();
			System.out.println("USER:" + userId);

			// loop for all cells of the users, get the cell with maximum numCheckin
			Map<String, CellData> userCellList = userCellsMap.get(userId);
			Iterator<String> cellIdIter = userCellList.keySet().iterator();
			String maxCellId = cellIdIter.next();
			CellData maxCell = userCellList.get(maxCellId);
			while( cellIdIter.hasNext() ){
				String cellId = cellIdIter.next();
				CellData newCell = userCellList.get(cellId);
				if( newCell.totalCheckin > maxCell.totalCheckin ){
					System.out.println( newCell.totalCheckin + ">" + maxCell.totalCheckin );
					maxCell = newCell;
					maxCellId = cellId;
				}
			}
			System.out.println("Max cell:" + maxCellId + "," + maxCell.totalCheckin );
			userTopCellMap.put(userId, maxCellId);
		}

		//loop every user and output the result, find the average location of the maximum cell
		System.out.println("===============================================\nOutputing average location..."  );
		Iterator<Map.Entry<Long,String>> topCellIter = userTopCellMap.entrySet().iterator();
		while( topCellIter.hasNext() ){
			Map.Entry<Long,String> entry = topCellIter.next();
			Long userId = entry.getKey();
			String cellId = entry.getValue();
			System.out.println("USER:" +userId + ", CELL:" + cellId );
			// get the cell and a set of lat/lngs
			CellData cellData = userCellsMap.get(userId).get(cellId);
			int numLocations = cellData.latLngList.size();
			//1) Convert each location to (x,y,z) coordinates, use the formulas  
			// x_n = cos(lat_n)*cos(lon_n)  
			// y_n = cos(lat_n)*sin(lon_n)  
			// z_n = sin(lat_n)
			//2) Average the coordinates independently:  
			//	 x = (x_1 + x_2 + x_3)/3  
			//	 y = (y_1 + y_2 + y_3)/3  
			//	 z = (z_1 + z_2 + z_3)/3
			//3) Now convert to latitude and longitude using the inverse transformation 
			//      r = sqrt(x^2 + y^2 + z^2)  
			//		 lat = arcsin(z/r)  
			//		 lon = arctan(y/x)
			double x_n = 0.0;
			double y_n = 0.0;
			double z_n = 0.0;

			for(LatLng latLng: cellData.latLngList){
				x_n += Math.cos(latLng.lat) * Math.cos(latLng.lng);
				y_n += Math.cos(latLng.lat) * Math.sin(latLng.lng);
				z_n += Math.sin(latLng.lat);
			}

			double x = x_n/numLocations;
			double y = y_n/numLocations;
			double z = z_n/numLocations;

			double r = Math.sqrt( x*x + y*y + z*z );
			double lat = Math.asin( z/r );
			double lng = Math.atan(y/x);

			System.out.println("LatLng set:" + cellData.latLngList.toString() );
			System.out.println( "=>" +  lat + "," + lng );

		}// end while
		 
	}
	
	/**
	 * Insert to MySQL DB, it's very slow. forget it
	 * @param relList
	 */
	public static void insertToDB(List<String> relList){
		
		for(int i = 0; i < relList.size(); i ++ ){
			String line = relList.get(i);
			String[] lineNodes = line.split("\\s+");
			UserCheckin o = new UserCheckin();
			o.userId = StringUtil.convertLong(lineNodes[0], -1 );
			o.spotId = lineNodes[4];
			o.createdAt = lineNodes[1].substring(0, lineNodes[1].length() -1);
			o.lat = StringUtil.convertDouble(lineNodes[2], 0 );
			o.lng = StringUtil.convertDouble(lineNodes[3], 0 );
			// TEST
			if( i == 10 ){
				break;
			}
			
			try {
				boolean ret = addUserCheckin( o, isGowalla );
				System.out.println(o.userId + "," + ret );
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	
	
	public static boolean addUserCheckin(UserCheckin o, boolean isGowalla) throws Exception
	{
		if( o == null )
		{
			return false;
		}

		// insert sql
		StringBuilder sql = new StringBuilder("insert into snapUserCheckins(user_id,spot_id,created_at,lat,lng) values(");
		sql.append(o.userId).append(",");
		if( isGowalla ){
			sql.append(o.spotId).append(",");
		}else{
			sql.append("\"").append(o.spotId).append("\",");
		}

		sql.append("\"").append(o.createdAt).append("\",");
		sql.append(o.lat).append(",");
		sql.append(o.lng);
		sql.append(")");

		try
		{
			systemLogger.debug("addUserCheckin,sql:\t"+ sql);
			(isGowalla ? DB_CONN1 : DB_CONN2).executeUpdate(sql.toString());
		}
		catch(SQLException e)
		{
			systemLogger.error("sql:" + sql,e);
			if( e.getMessage().indexOf("Duplicate") > -1 ){
				throw e;
			}else{
				return false;
			}
		}

		return true;
	}
	
	static class UserCheckin{
		long id;
		long userId;
		String spotId;
		String createdAt;
		double lat;
		double lng;
		int numCheckin;
	}
	
	static class CellData{
		int x;
		int y;
		List<LatLng> latLngList = new ArrayList<LatLng>();
		int totalCheckin;
		public String toString(){
			String sb = "";
			for( LatLng l : latLngList){
				sb += l + "|";
			}
			return sb;
		}
	}
	
	static class LatLng{
		double lat;
		double lng;
		public LatLng(double lat, double lng ){
			this.lat = lat;
			this.lng = lng;
		}
		public String toString(){
			return this.lat + "," + this.lng;
		}
	}
}
