package jdbc.stream.iterator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

/**
 * ResultSetIterator is an Iterator implementation of the JDBC ResultSet class. This iterator is used
 * for creating Stream implementation of the JDBC ResultSet class.
 * 
 * @author Julio Pineda
 *
 */
public class ResultSetIterator implements Iterator<ResultSet> {
	private Connection connection;
	private String sql;
	
	private PreparedStatement preparedStatement;
	private ResultSet resultSet;
	
	public ResultSetIterator(Connection connection, String sql) {
		this.connection = connection;
		this.sql = sql;
	}
	
	/**
	 * Checks if ResultSet has more rows and moves the pointer of the ResultSet.
	 */
	@Override
	public boolean hasNext() {
		if (this.preparedStatement == null) {
			this.initialize();
		}
		
		boolean hasNext = false;
		
		try {
			hasNext = resultSet.next();
		}
		catch (SQLException e) {
			close();
			e.printStackTrace();
		}
		
		return hasNext;
	}
	
	/**
	 * Return the ResultSet pointer.
	 */
	@Override
	public ResultSet next() {
		return this.resultSet;
	}
	
	/**
	 * Prepares and executes SQL query and prepares iteration through ResultSet
	 */
	private void initialize() {
		try {
			this.preparedStatement = connection.prepareStatement(this.sql);
			this.resultSet = this.preparedStatement.executeQuery();
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
}
