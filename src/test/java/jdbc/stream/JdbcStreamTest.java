package jdbc.stream;

import static org.junit.Assert.assertEquals;

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

public class JdbcStreamTest {
	private static Connection connection;
	private static String sqlQuery = "SELECT * FROM iris";
	
	private static long rowNum;
	private static double aveSepalLength;
	private static String widestPetal;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		connection = DriverManager.getConnection("jdbc:sqlite::memory:");
		createIrisTable(connection);
		insertIrisData(connection);
		
		PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery);
		ResultSet resultSet = preparedStatement.executeQuery();
		
		long num = 0;
		List<Double> sepalLengths = new ArrayList<>();
		Map<String, List<Double>> petalWidths = new HashMap<>();
		while (resultSet.next()) {
			num++;
			
			double sepalLength = resultSet.getDouble("SepalLength");
			double petalWidth = resultSet.getDouble("PetalWidth");
			String irisClass = resultSet.getString("IrisClass");
			
			sepalLengths.add(sepalLength);
			petalWidths.computeIfAbsent(irisClass, key -> new ArrayList<>()).add(petalWidth);
		}
		
		rowNum = num;
		aveSepalLength = sepalLengths.stream().mapToDouble(i -> i).average().getAsDouble();
		
		widestPetal = "";
		double widestPetalWidth = 0;
		for (Map.Entry<String, List<Double>> entry : petalWidths.entrySet()) {
			String irisClass = entry.getKey();
			double max = Collections.max(entry.getValue());
			
			if (max > widestPetalWidth) {
				widestPetalWidth = max;
				widestPetal = irisClass;
			}
		}
	}
	
	private static void createIrisTable(Connection connection) throws SQLException {
		String sqlTable = "CREATE TABLE iris ( \n" + "    SepalLength real, \n"
						+ "    SepalWidth real, \n" + "    PetalLength real, \n"
						+ "    PetalWidth real, \n" + "    IrisClass varchar(255) \n" + ");";
		
		PreparedStatement preparedStatement = connection.prepareStatement(sqlTable);
		preparedStatement.execute();
	}
	
	private static void insertIrisData(Connection connection) throws SQLException {
		Path irisPath = null;
		try {
			irisPath = Paths.get(TestSQL.class.getClassLoader().getResource("iris.data").toURI());
		}
		catch (URISyntaxException e1) {
			e1.printStackTrace();
		}
		
		try (BufferedReader reader = Files.newBufferedReader(irisPath)) {
			String line;
			while ((line = reader.readLine()) != null) {
				String[] split = line.split(",");
				if (split.length == 0) {
					continue;
				}
				
				double sepalLength = Double.parseDouble(split[0]);
				double sepalWidth = Double.parseDouble(split[1]);
				double petalLength = Double.parseDouble(split[2]);
				double petalWidth = Double.parseDouble(split[3]);
				String irisClass = split[4];
				
				insert(connection, sepalLength, sepalWidth, petalLength, petalWidth, irisClass);
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
	
	@Test
	public void rowNumTest() throws SQLException {
		long count = JdbcStream.stream(connection, sqlQuery).count();
		
		assertEquals(rowNum, count);
	}
	
	@Test
	public void averageTest() throws SQLException {
		double streamAveSepalLength = JdbcStream.stream(connection, sqlQuery).mapToDouble(rs -> {
			double sepalLength = 0;
			
			try {
				sepalLength = rs.getDouble("SepalLength");
			}
			catch (SQLException e) {
				e.printStackTrace();
			}
			
			return sepalLength;
		}).average().getAsDouble();
		
		assertEquals(aveSepalLength, streamAveSepalLength, 0.01);
	}
	
	@Test
	public void widestPetalTest() throws SQLException {
		//		JdbcStream.stream(connection, sqlQuery).map(rs -> {
		//			String irisClass = "";
		//			double petalWidth = 0;
		//			try {
		//				irisClass = rs.getString("IrisClass");
		//				petalWidth = rs.getDouble("PetalWidth");
		//			}
		//			catch (SQLException e) {
		//				e.printStackTrace();
		//			}
		//			
		//			return new AbstractMap.SimpleEntry<>(irisClass, petalWidth);
		//		}).collect);
	}
}
