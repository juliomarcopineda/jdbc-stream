package jdbc.stream;

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
	private String idField;
	
	private PreparedStatement preparedStatement;
	private ResultSet resultSet;
	private List<String> columnNames;
	
	private Map<String, Object> before;
	
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
	
	/**
	 * Prepares and executes SQL query and prepares iteration through ResultSet
	 */
	private void initialize() {
		try {
			this.preparedStatement = connection.prepareStatement(this.sql);
			this.resultSet = this.preparedStatement.executeQuery();
			this.columnNames = getColumnNames(this.resultSet);
			
			before = new LinkedHashMap<>();
			setValues(before);
			
		}
		catch (SQLException e) {
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
	 * @param row
	 */
	private void setValues(Map<String, Object> row) {
		for (String columnName : this.columnNames) {
			try {
				Object value = this.resultSet.getObject(columnName);
				row.put(columnName, value);
			}
			catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Returns the List of columns names given a open result set.
	 * 
	 * @param resultSet
	 * @return list of column names
	 */
	private List<String> getColumnNames(ResultSet resultSet) {
		List<String> columnNames = new ArrayList<>();
		
		try {
			ResultSetMetaData metaData = resultSet.getMetaData();
			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				columnNames.add(metaData.getColumnLabel(i));
			}
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
		
		return columnNames;
	}
}
