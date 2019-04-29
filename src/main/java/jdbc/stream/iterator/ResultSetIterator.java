package jdbc.stream.iterator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 
 * 
 * @author Julio Pineda
 *
 */
public class ResultSetIterator implements Iterator<Map<String, Object>> {
	private Connection connection;
	private String sql;
	
	private PreparedStatement preparedStatement;
	private ResultSet resultSet;
	
	private List<String> columnLabels;
	
	public ResultSetIterator(Connection connection, String sql) {
		this.connection = connection;
		this.sql = sql;
	}
	
	/**
	 * 
	 */
	@Override
	public boolean hasNext() {
		boolean hasMore = true;
		
		if (this.preparedStatement == null) {
			this.initialize();
		}
		
		try {
			if (!resultSet.next()) {
				hasMore = false;
			}
		}
		catch (SQLException e) {
			close();
			e.printStackTrace();
		}
		
		return hasMore;
	}
	
	/**
	 * 
	 */
	@Override
	public Map<String, Object> next() {
		Map<String, Object> map = new LinkedHashMap<>();
		
		for (String columnLabel : this.columnLabels) {
			try {
				map.put(columnLabel, this.resultSet.getObject(columnLabel));
			}
			catch (SQLException e) {
				close();
				e.printStackTrace();
			}
		}
		
		return map;
	}
	
	/**
	 * Prepares and executes SQL query and prepares iteration through ResultSet
	 */
	private void initialize() {
		try {
			this.preparedStatement = connection.prepareStatement(this.sql);
			this.resultSet = this.preparedStatement.executeQuery();
			setMetaData();
		}
		catch (SQLException e) {
			this.close();
			e.printStackTrace();
		}
	}
	
	/**
	 * Close the ResultSet and PreparedStatment resources
	 */
	private void close() {
		try {
			this.resultSet.close();
			this.preparedStatement.close();
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 
	 */
	private void setMetaData() {
		this.columnLabels = new ArrayList<>();
		
		try {
			ResultSetMetaData metaData = resultSet.getMetaData();
			
			for (int column = 1; column <= metaData.getColumnCount(); column++) {
				this.columnLabels.add(metaData.getColumnLabel(column));
			}
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
