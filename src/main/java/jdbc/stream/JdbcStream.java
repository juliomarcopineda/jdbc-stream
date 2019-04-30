package jdbc.stream;

import java.sql.ResultSet;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import jdbc.stream.iterator.ResultSetSpliterator;

/**
 * JdbcStream is a Stream wrapper on top of the JDBC ResultSet. This class provides utility methods to convert
 * the ResultSet into a Stream<ResultSet> which opens up the Java Stream API to JDBC.
 * 
 * @author pinedajb
 *
 */
public class JdbcStream {
	
	/**
	 * Given a ResultSet, convert the ResultSet into a Stream
	 * 
	 * @param resultSet
	 * @return
	 */
	public static Stream<ResultSet> stream(ResultSet resultSet) {
		ResultSetSpliterator spliterator = new ResultSetSpliterator(resultSet);
		
		return StreamSupport.stream(spliterator, false);
	}
	
	private JdbcStream() {
	}
}
