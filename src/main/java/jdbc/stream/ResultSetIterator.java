package jdbc.stream;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Iterator;
import java.util.Map;

public class ResultSetIterator implements Iterator<Map<String, Object>> {
	private Connection connection;
	private String sql;
	private String idField;
	
	private PreparedStatement preparedStatement;
	private ResultSet resultSet;
	
	public ResultSetIterator(Connection connection, String sql, String idField) {
		this.connection = connection;
		this.sql = sql;
		this.idField = idField;
	}
	
	@Override
	public boolean hasNext() {
		boolean hasMore = false;
		
		if (this.preparedStatement == null) {
			initialize();
		}
		
		return false;
	}
	
	@Override
	public Map<String, Object> next() {
		// TODO Auto-generated method stub
		return null;
	}
	
	private void initialize() {
		
	}
	
	private void close() {
		
	}
}
