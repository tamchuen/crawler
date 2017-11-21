package com.tamchuen.crawler.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.tamchuen.jutil.j4log.Logger;
import com.tamchuen.jutil.string.StringUtil;
import com.tamchuen.jutil.util.FileUtil;

/**
 * Process the text based checkin data from gowalla and brightkite
 * @author  Dequan
 * Project: Crawler
 * Date:    Apr 30, 2012
 * 
 */
public class DetectHomeOffice {
	private static Logger systemLogger = Logger.getLogger("snap_data");
	private static String gowallaCheckinFile = "E:\\My Documents\\Study\\Recommender\\Summer2012\\Gowalla_totalCheckins.txt";
	private static String brightkiteCheckinFile = "E:\\My Documents\\Study\\Recommender\\Summer2012\\Brightkite_totalCheckins.txt";
	
	// user spots for home/office hours
	private static String brightkiteUserSpotsFileOffice = "E:\\My Documents\\Study\\Recommender\\Summer2012\\0523\\Brightkite_userSpots2_office.txt";
	private static String brightkiteUserSpotsFileHome = "E:\\My Documents\\Study\\Recommender\\Summer2012\\0523\\Brightkite_userSpots2_home.txt";
	
	private static String gowallaUserSpotsFileOffice = "E:\\My Documents\\Study\\Recommender\\Summer2012\\0523\\Gowalla_userSpots2_office.txt";
	private static String gowallaUserSpotsFileHome = "E:\\My Documents\\Study\\Recommender\\Summer2012\\0523\\Gowalla_userSpots2_home.txt";
	private static String outputFolder = "E:\\My Documents\\Study\\Recommender\\Gowalla Data-With-Category\\Gowalla\\";
	
	// type, 1 : Gowalla, 2 :Brightkite 
	private static int type = 1;
	
	public static void main(String args[]){
		type = 1;
		gowallaUserSpotsFileHome = "E:\\My Documents\\Study\\Recommender\\Gowalla Data-With-Category\\Gowalla\\Gowalla_userSpots2_home.txt";
		gowallaUserSpotsFileOffice = "E:\\My Documents\\Study\\Recommender\\Gowalla Data-With-Category\\Gowalla\\Gowalla_userSpots2_office.txt";
		
		detectResultFromTwoFiles();
		
	}
	
	public static void detectResultFromOneFile(){
		// 5PM - 9 AM : home
		int homeBegin = 17;
		int homeEnd = 9; 
		// 9 AM - 5PM : office
		int officeBegin = homeEnd;
		int officeEnd = homeBegin;
		
	}
	
	public static void detectResultFromTwoFiles(){
		//String inputFile = isGowalla ? gowallaCheckinFile : brightkiteCheckinFile;
				String inputFileOffice = type ==1 ? gowallaUserSpotsFileOffice : brightkiteUserSpotsFileOffice;
				String inputFileHome = type ==1 ? gowallaUserSpotsFileHome : brightkiteUserSpotsFileHome;
				
				// Read file 
				List<String> relListOffice = FileUtil.readFile2List(inputFileOffice);
				if( relListOffice == null || relListOffice.size() == 0 ){
					System.err.println("Office File not exists!");
					System.exit(0);
				}
				List<String> relListHome = FileUtil.readFile2List(inputFileHome);
				if( relListHome == null || relListHome.size() == 0 ){
					System.err.println("Home File not exists!");
					System.exit(0);
				}
				
				// Detect the HOME
				System.out.println("Start for " + relListOffice.size() + " Home records of " + (type ==1 ? "Gowalla" : "Brightkite") );
				
				long start = System.currentTimeMillis();
				
				Map<Long, LatLng> userHomeMap = new HashMap<Long, LatLng>();
				
				DoDetectHomeOffice(relListHome, userHomeMap);
				
				long end = System.currentTimeMillis();
				int elapsed = (int) (end-start)/1000;
				System.out.println("Finished for " + relListHome.size() + " records, time: " + elapsed + "s");
				
				// Detect the OFFICE
				System.out.println("Start for " + relListOffice.size() + " Home records of " + (type ==1 ? "Gowalla" : "Brightkite") );
				
				start = System.currentTimeMillis();
				
				Map<Long, LatLng> userOfficeMap = new HashMap<Long, LatLng>();
				
				DoDetectHomeOffice(relListOffice, userOfficeMap);
				
				end = System.currentTimeMillis();
			    elapsed = (int) (end-start)/1000;
				System.out.println("Finished for " + relListOffice.size() + " Office records, time: " + elapsed + "s");
				
				// generate the result
				System.out.println("Output the results.");
				Iterator<Long> userIdIter = userOfficeMap.keySet().iterator();
				
				// loop for all users
				StringBuilder sb = new StringBuilder("uid, homeLat, homeLng, officeLat, officeLng\r\n");
				while( userIdIter.hasNext() ){
					Long userId = userIdIter.next();
					LatLng home = userHomeMap.get(userId);
					LatLng office = userOfficeMap.get(userId);
					if( home != null && office != null ){
						sb.append(userId + "," + home.toString() + "," + office.toString() + "\r\n");
					}
				}
				
				// save the result to file
				FileUtil.writeFile( outputFolder + (type ==1 ? "Gowalla" : "Brightkite") + "HomeOffice.txt", sb.toString() );
			
		
	}
	/**
	 * Detect Home/Office location, first convert the lat/lng into 25kmx25km grid,
	 * then find out the cell with the most #check-ins of each user, then find the average position inside that cell
	 * @param relList
	 * @param userLocationMap user-home or user-office map
	 */
	public static void DoDetectHomeOffice(List<String> relList, Map<Long, LatLng> userLocationMap){
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

			// Test
			//if( i == 12500 ){
			//	break;
			//}

			try {
				// convert the lng,lat into X,Y
				// Xbin = [lng/theta] , Ybin = [lat/theta], should convert to integer
				int x_bin = (int) Math.floor( o.lng / theta );
				int y_bin = (int) Math.floor( o.lat / theta );
				String cellId =  x_bin + "|" + y_bin;
				//System.out.println(o.userId + "," + cellId  );
				
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
					//System.out.println("add user:" + o.userId   );
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
						//System.out.println("add cell:" + o.userId + "," + cellId  );
					}else{
						// cell exists, just update and add the latlng into the list
						cellData.totalCheckin += o.numCheckin;
						cellData.latLngList.add( new LatLng(o.lat, o.lng ) );
						//System.out.println("add latlng:" + o.userId + "," + cellId + "," + o.lat + "," + o.lng );
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
			//System.out.println("USER:" + userId);

			// loop for all cells of the users, get the cell with maximum numCheckin
			Map<String, CellData> userCellList = userCellsMap.get(userId);
			Iterator<String> cellIdIter = userCellList.keySet().iterator();
			String maxCellId = cellIdIter.next();
			CellData maxCell = userCellList.get(maxCellId);
			while( cellIdIter.hasNext() ){
				String cellId = cellIdIter.next();
				CellData newCell = userCellList.get(cellId);
				if( newCell.totalCheckin > maxCell.totalCheckin ){
					//System.out.println( newCell.totalCheckin + ">" + maxCell.totalCheckin );
					maxCell = newCell;
					maxCellId = cellId;
				}
			}
			//System.out.println("Max cell:" + maxCellId + "," + maxCell.totalCheckin );
			userTopCellMap.put(userId, maxCellId);
		}

		int debugCount = 0;
		//loop every user and output the result, find the average location of the maximum cell
		System.out.println("===============================================\nOutputing average location..."  );
		Iterator<Map.Entry<Long,String>> topCellIter = userTopCellMap.entrySet().iterator();
		while( topCellIter.hasNext() ){
			Map.Entry<Long,String> entry = topCellIter.next();
			Long userId = entry.getKey();
			String cellId = entry.getValue();
			//System.out.println("USER:" +userId + ", CELL:" + cellId );
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

			// need to convert lat/lng from degree to radians first
			
			for(LatLng latLng: cellData.latLngList){
				double lat1 = latLng.lat;
				double lng1 = latLng.lng;
				
				lat1 = (lat1 * Math.PI )/180;
				lng1 = (lng1 * Math.PI )/180;
				
				x_n += Math.cos(lat1) * Math.cos(lng1);
				y_n += Math.cos(lat1) * Math.sin(lng1);
				z_n += Math.sin(lat1);
			}

			double x = x_n/numLocations;
			double y = y_n/numLocations;
			double z = z_n/numLocations;

			double r = Math.sqrt( x*x + y*y + z*z );
			double lat_radian = Math.asin( z/r );
			double lng_radian = Math.atan(y/x);

			// convert from radians to degrees , notice the positive/negative sign changes
			double lat_deg = (lat_radian * 180)/Math.PI;
			double lng_deg = (lng_radian * 180)/Math.PI;
			
			
			// lat/lng should have the same sign as original one
			double lat_deg_1 = cellData.latLngList.get(0).lat;
			double lng_deg_1 = cellData.latLngList.get(0).lng;
			if( lng_deg_1 < 0 && lng_deg >0  ){
				lng_deg = lng_deg - 180;
			}
			if( lng_deg_1 > 0 && lng_deg < 0  ){
				lng_deg = lng_deg + 180;
			}
			
			// debug data
			if( debugCount < 10 && userId % 1000 == 0 ){
				debugCount ++;
				System.out.println("LatLng set " + userId + ":\n" + cellData.toString() );
				System.out.println(  lat_deg + "," + lng_deg );
			}
			
			// put the output result 
			userLocationMap.put(userId, new LatLng(lat_deg,lng_deg) );
		}// end while
		 
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
