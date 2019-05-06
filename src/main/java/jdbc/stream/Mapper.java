package jdbc.stream;

import java.sql.ResultSet;

/**
 * Interface to define a Mapper class that maps a row from the ResultSet to any client-defined class.
 * 
 * @author Julio Marco Pineda
 *
 * @param <TEntity>
 */
public interface Mapper<TEntity> {
	
	/**
	 * Given a ResultSet, maps the data in the ResultSet row into a client-defined class.
	 * 
	 * @param resultSet
	 * @return custom client class
	 */
	public TEntity map(ResultSet resultSet);
}
