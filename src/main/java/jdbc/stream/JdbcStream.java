package jdbc.stream;

import java.sql.ResultSet;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * JdbcStream is a Stream wrapper on top of the JDBC ResultSet. This class provides utility methods to convert
 * the ResultSet into a Stream of ResultSet which opens up the Java Stream API to JDBC.
 * 
 * @author Julio Marco Pineda
 *
 */
public class JdbcStream {
	
	/**
	 * Convert the ResultSet into a Stream given the JDBC ResultSet.
	 * 
	 * @param resultSet The JDBC ResultSet after executing SQL query
	 * @return Stream of ResultSet
	 */
	public static Stream<ResultSet> stream(ResultSet resultSet) {
		ResultSetSpliterator spliterator = new ResultSetSpliterator(resultSet);
		
		return StreamSupport.stream(spliterator, false);
	}
	
	/**
	 * Convert the ResultSet into a Stream of the client-defined class using a custom Mapper.
	 * 
	 * @param resultSet
	 * @param mapper
	 * @return Stream of custom client-defined class
	 */
	public static <TEntity> Stream<TEntity> stream(ResultSet resultSet, Mapper<TEntity> mapper) {
		ResultSetSpliterator spliterator = new ResultSetSpliterator(resultSet);
		
		return StreamSupport.stream(spliterator, false).map(mapper::map);
	}
	
	private JdbcStream() {
	}
}
