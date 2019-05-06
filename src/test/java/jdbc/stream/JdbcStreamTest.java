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
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests of the JdbcStream library. These tests ensure that traversing the ResultSet without the JdbcStream wrapper
 * is the same as traversing using the JdbcStream library and API.
 * 
 * @author pinedajb
 *
 */
public class JdbcStreamTest {
	private static Connection connection;
	private static String sqlQuery = "SELECT * FROM iris";
	
	private static long rowNum;
	private static double aveSepalLength;
	private static String widestPetal;
	private static double aveSepalArea;
	
	/**
	 * <p>Creates a SQLite instance in memory, creates a table with a pre-defined scheme and inserts the
	 * Iris data set for testing the JdbcStream library.</p>
	 * 
	 * <p>Additionally, performs calculations on the Iris data set by using traversing the ResultSet without
	 * the Stream API which will be used for testing</p>
	 * 
	 * The Iris data set can be found from:
	 * <a href=https://archive.ics.uci.edu/ml/datasets/iris>https://archive.ics.uci.edu/ml/datasets/iris</a>
	 * 
	 * @throws Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		connection = DriverManager.getConnection("jdbc:sqlite::memory:");
		createIrisTable(connection);
		insertIrisData(connection);
		
		PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery);
		ResultSet resultSet = preparedStatement.executeQuery();
		
		long num = 0;
		List<Double> sepalLengths = new ArrayList<>();
		List<Double> sepalWidths = new ArrayList<>();
		Map<String, List<Double>> petalWidths = new HashMap<>();
		while (resultSet.next()) {
			num++;
			
			double sepalLength = resultSet.getDouble("SepalLength");
			double sepalWidth = resultSet.getDouble("SepalWidth");
			double petalWidth = resultSet.getDouble("PetalWidth");
			String irisClass = resultSet.getString("IrisClass");
			
			sepalLengths.add(sepalLength);
			sepalWidths.add(sepalWidth);
			petalWidths.computeIfAbsent(irisClass, key -> new ArrayList<>()).add(petalWidth);
		}
		
		rowNum = num;
		aveSepalLength = sepalLengths.stream().mapToDouble(i -> i).average().getAsDouble();
		
		widestPetal = petalWidths.entrySet()
						.stream()
						.max((e1, e2) -> Collections.max(e1.getValue()) > Collections
										.max(e2.getValue()) ? 1 : -1)
						.get()
						.getKey();
		
		List<Double> sepalAreas = new ArrayList<>();
		for (int i = 0; i < sepalLengths.size(); i++) {
			double sepalLength = sepalLengths.get(i);
			double sepalWidth = sepalWidths.get(i);
			
			sepalAreas.add(sepalLength * sepalWidth);
		}
		aveSepalArea = sepalAreas.stream().mapToDouble(i -> i).average().getAsDouble();
	}
	
	/**
	 * Given the SQLite connection, creates a schema for the Iris data set and a creates a table.
	 * 
	 * @param connection
	 * @throws SQLException
	 */
	private static void createIrisTable(Connection connection) throws SQLException {
		String sqlTable = "CREATE TABLE iris ( \n" + "    SepalLength real, \n"
						+ "    SepalWidth real, \n" + "    PetalLength real, \n"
						+ "    PetalWidth real, \n" + "    IrisClass varchar(255) \n" + ");";
		
		PreparedStatement preparedStatement = connection.prepareStatement(sqlTable);
		preparedStatement.execute();
	}
	
	/**
	 * Given the SQLite connection, inserts the Iris data into a table.
	 * 
	 * @param connection
	 * @throws SQLException
	 */
	private static void insertIrisData(Connection connection) throws SQLException {
		Path irisPath = null;
		try {
			irisPath = Paths.get(
							JdbcStreamTest.class.getClassLoader().getResource("iris.data").toURI());
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
	
	/**
	 * Inserts the different data fields of the Iris data set into the iris table.
	 * 
	 * @param conn
	 * @param sepalLength
	 * @param sepalWidth
	 * @param petalLength
	 * @param petalWidth
	 * @param irisClass
	 * @throws SQLException
	 */
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
	
	/**
	 * Counts the number of rows in the iris data set using the JdbcStream library and Stream API. Then,
	 * tests if this count is correct.
	 * 
	 * @throws SQLException
	 */
	@Test
	public void rowNumTest() throws SQLException {
		PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery);
		ResultSet resultSet = preparedStatement.executeQuery();
		
		long count = JdbcStream.stream(resultSet).count();
		
		assertEquals(rowNum, count);
	}
	
	/**
	 * Calculates the average Sepal Length of all the observations in the Iris data sets using the JdbcStream library
	 * and Stream API. Tests if this average is correctly computed.
	 * 
	 * @throws SQLException
	 */
	@Test
	public void averageTest() throws SQLException {
		PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery);
		ResultSet resultSet = preparedStatement.executeQuery();
		
		double streamAveSepalLength = JdbcStream.stream(resultSet).mapToDouble(rs -> {
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
	
	/**
	 * Determines which Iris species has the widest Petal Width using the JdbcStream library and Stream API.
	 * Checks if this determination is correct.
	 * 
	 * @throws SQLException
	 */
	@Test
	public void widestPetalTest() throws SQLException {
		PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery);
		ResultSet resultSet = preparedStatement.executeQuery();
		
		Map<String, List<Double>> petalWidths = JdbcStream.stream(resultSet).map(rs -> {
			String irisClass = "";
			double petalWidth = 0;
			try {
				irisClass = rs.getString("IrisClass");
				petalWidth = rs.getDouble("PetalWidth");
			}
			catch (SQLException e) {
				e.printStackTrace();
			}
			
			return new AbstractMap.SimpleEntry<>(irisClass, petalWidth);
		}).collect(Collectors.toMap(Map.Entry::getKey, e -> {
			List<Double> list = new ArrayList<>();
			list.add(e.getValue());
			return list;
		}, (l1, l2) -> Stream.of(l1, l2).flatMap(Collection::stream).collect(Collectors.toList())));
		
		String widestPetalTest = petalWidths.entrySet()
						.stream()
						.max((e1, e2) -> Collections.max(e1.getValue()) > Collections
										.max(e2.getValue()) ? 1 : -1)
						.get()
						.getKey();
		
		assertEquals(widestPetal, widestPetalTest);
	}
	
	@Test
	public void mapperTest() throws SQLException {
		PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery);
		ResultSet resultSet = preparedStatement.executeQuery();
		
		// Custom class to contain data about the Sepal
		class Sepal {
			double width;
			double length;
			
			double getArea() {
				return width * length;
			}
		}
		
		Mapper<Sepal> areaMapper = new Mapper<Sepal>() {
			
			@Override
			public Sepal map(ResultSet resultSet) {
				Sepal sepal = new Sepal();
				
				try {
					sepal.length = resultSet.getDouble("SepalLength");
					sepal.width = resultSet.getDouble("SepalWidth");
				}
				catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				return sepal;
			}
		};
		
		double average = JdbcStream.stream(resultSet, areaMapper)
						.map(Sepal::getArea)
						.mapToDouble(i -> i)
						.average()
						.getAsDouble();
		
		assertEquals(aveSepalArea, average, 0.01);
	}
	
	/**
	 * Closes all resources after all tests have been performed.
	 */
	@AfterClass
	public static void tearDownAfterClass() throws SQLException {
		connection.close();
	}
}
