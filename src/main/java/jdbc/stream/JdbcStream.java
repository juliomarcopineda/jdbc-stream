package jdbc.stream;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import jdbc.stream.iterator.ResultSetIterator;

public class JdbcStream {
	
	public static Stream<ResultSet> stream(Connection connection, String sql) {
		ResultSetIterator iterator = new ResultSetIterator(connection, sql);
		
		return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
	}
	
	private JdbcStream() {
	}
}
