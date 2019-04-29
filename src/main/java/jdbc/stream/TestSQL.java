package jdbc.stream;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TestSQL {
	public static void main(String[] args) {
		try {
			Connection conn = DriverManager.getConnection("jdbc:sqlite::memory:");
			
			String sqlTable = "CREATE TABLE iris ( \n" + "    SepalLength real, \n"
							+ "    SepalWidth real, \n" + "    PetalLength real, \n"
							+ "    PetalWidth real, \n" + "    Class varchar(255) \n" + ");";
			
			PreparedStatement preparedStatement = conn.prepareStatement(sqlTable);
			preparedStatement.execute();
			
			ResultSet rs = conn.getMetaData().getTables(null, null, "%", null);
			while (rs.next()) {
				System.out.println(rs.getString(3));
			}
		}
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
