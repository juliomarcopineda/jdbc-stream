package jdbc.stream;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TestSQL {
	public static void main(String[] args) {
		try {
			Connection conn = createConnection();
			createIrisTable(conn);
			insertIrisData(conn);
			
			String sql = "SELECT * FROM iris";
			PreparedStatement ps = conn.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();
			
			while (rs.next()) {
				System.out.println(rs.getString("IrisClass") + "\t" + rs.getDouble("SepalLength"));
			}
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	private static void insertIrisData(Connection conn) throws SQLException {
		Path irisPath = null;
		try {
			irisPath = Paths.get(TestSQL.class.getClassLoader().getResource("iris.data").toURI());
		}
		catch (URISyntaxException e1) {
			e1.printStackTrace();
		}
		
		try (BufferedReader reader = Files.newBufferedReader(irisPath)) {
			String line;
			int i = 0;
			while ((line = reader.readLine()) != null) {
				i++;
				String[] split = line.split(",");
				if (split.length == 0) {
					continue;
				}
				
				double sepalLength = Double.parseDouble(split[0]);
				double sepalWidth = Double.parseDouble(split[1]);
				double petalLength = Double.parseDouble(split[2]);
				double petalWidth = Double.parseDouble(split[3]);
				String irisClass = split[4];
				
				insert(conn, sepalLength, sepalWidth, petalLength, petalWidth, irisClass);
			}
		}
		catch (IOException e1) {
			e1.printStackTrace();
		}
	}
	
	private static void insert(Connection conn, double sepalLength, double sepalWidth,
					double petalLength, double petalWidth, String irisClass) throws SQLException {
		String sql = "INSERT INTO iris(SepalLength, SepalWidth, PetalLength, PetalWidth, IrisClass) VALUES(?,?,?,?,?)";
		
		PreparedStatement preparedStatement = conn.prepareStatement(sql);
		preparedStatement.setDouble(1, sepalLength);
		preparedStatement.setDouble(2, sepalWidth);
		preparedStatement.setDouble(3, petalLength);
		preparedStatement.setDouble(4, petalWidth);
		preparedStatement.setString(5, irisClass);
		
		preparedStatement.executeUpdate();
	}
	
	private static void createIrisTable(Connection conn) throws SQLException {
		String sqlTable = "CREATE TABLE iris ( \n" + "    SepalLength real, \n"
						+ "    SepalWidth real, \n" + "    PetalLength real, \n"
						+ "    PetalWidth real, \n" + "    IrisClass varchar(255) \n" + ");";
		
		PreparedStatement preparedStatement = conn.prepareStatement(sqlTable);
		preparedStatement.execute();
	}
	
	private static Connection createConnection() throws SQLException {
		return DriverManager.getConnection("jdbc:sqlite::memory:");
	}
}
