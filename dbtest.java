package db;

import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import module.Products;



public class dbtest {
	public static void main(String[] args) throws UnsupportedEncodingException {
		DBUtils db = new DBUtils();
		String sql = "SELECT * FROM record where URL like '%.HTML' ORDER BY `record`.`recordID` ASC";
		//SELECT * FROM `record` where URL like '%.HTML'
		ResultSet result = db.query(sql);
		int i=0;
		try {
			while(result.next()){
				//Products p = new Products();
				
				i++;
				System.out.print(i+"=");
				System.out.print(result.getInt("recordID"));
				System.out.print(":");
				System.out.println(result.getString("URL"));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
