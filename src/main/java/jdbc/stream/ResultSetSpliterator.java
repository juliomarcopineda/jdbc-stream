package jdbc.stream;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * ResultSetSpliterator is a light-weight wrapper on top of the JDBC ResultSet class. This class handles iterating through
 * the ResultSet, and allows the creation of a Stream<ResultSet> to take advantage of the Stream API.
 * 
 * @author pinedajb
 *
 */
public class ResultSetSpliterator implements Spliterator<ResultSet> {
	private ResultSet resultSet;
	
	public ResultSetSpliterator(ResultSet resultSet) {
		this.resultSet = resultSet;
	}
	
	@Override
	public boolean tryAdvance(Consumer<? super ResultSet> action) {
		try {
			if (this.resultSet.next()) {
				action.accept(this.resultSet);
				return true;
			}
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
		
		return false;
	}
	
	@Override
	public Spliterator<ResultSet> trySplit() {
		return null;
	}
	
	@Override
	public long estimateSize() {
		return 0;
	}
	
	@Override
	public int characteristics() {
		return 0;
	}
}
