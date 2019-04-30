package jdbc.stream;

import java.sql.ResultSet;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * JdbcStream is a Stream wrapper on top of the JDBC ResultSet. This class provides utility methods to convert
 * the ResultSet into a Stream of ResultSet which opens up the Java Stream API to JDBC.
 * 
 * @author pinedajb
 *
 */
public class JdbcStream {
	
	/**
	 * Given a ResultSet, convert the ResultSet into a Stream
	 * 
	 * @param resultSet The JDBC ResultSet after executing SQL query
	 * @return Stream of ResultSet
	 */
	public static Stream<ResultSet> stream(ResultSet resultSet) {
		ResultSetSpliterator spliterator = new ResultSetSpliterator(resultSet);
		
		return StreamSupport.stream(spliterator, false);
	}
	
	private JdbcStream() {
	}
}
