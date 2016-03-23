
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

public class HiveManager{	 
	  private static final String driver = "org.apache.hive.jdbc.HiveDriver";
	
	public static Connection getConnection() throws Exception {
		Class.forName(driver);
		Connection con = null;
	    	try {
	    		System.out.println("trying to connect with hive ");
			con = DriverManager.getConnection("jdbc:hive2://dbk-hm3.uncc.edu:10000/rgottam1;principal=hive/dbk-hm3.uncc.edu@URC.UNCC.EDU", "", "");
	    		System.out.println("Connected successfully");
	    		return con;
	    	 	
	    	} catch (Exception e) {
	    		e.printStackTrace();
	    		return null;
	    	
		}
	}
	
	public static String[] getItemItemRecommendations(String itemId){
		String[] recList = null;
		try {
			Connection con = getConnection();
			Statement stmt = con.createStatement();
    		// show tables
    		String sql = " select recList from iirecomm where itemId = "+itemId;
    		System.out.println("Running: " + sql);
    		ResultSet res = stmt.executeQuery(sql);
    	 	while (res.next()) {
    	 		recList = res.getString(0).split("\\s*");
    	 		
    		}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return recList;
	}
	
	public static String[] getUserUserRecommendations(String userId){
		String[] recList = null;
		try {
			Connection con = getConnection();
			Statement stmt = con.createStatement();
    		// show tables
    		String sql = " select recList from uurecomm where userId = "+userId;
    		System.out.println("Running: " + sql);
    		ResultSet res = stmt.executeQuery(sql);
    	 	if (res.next()) {
    	 		recList = res.getString(0).split("\\s*");
    	 		
    		}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return recList;
	}
		
		public static String getbookTitle(String bookId){
			String title="";
			try {
				Connection con = getConnection();
				Statement stmt = con.createStatement();
				String sql = "select itemtitle from book_details where itemid ="+bookId;
	    		System.out.println("Running: " + sql);
	    		
	    		ResultSet res = stmt.executeQuery(sql);
	    	 	if (res.next()) {
	    	 		title = res.getString(1);
	    		}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		return title;
		
	}
}
