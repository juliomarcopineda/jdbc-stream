package jdbc.stream.row;

import java.util.LinkedHashMap;

public class Row extends LinkedHashMap<String, Object> {
	private static final long serialVersionUID = -8036034260912142659L;
	
	@Override
	public Object put(String key, Object value) {
		
		return super.put(key, value);
	}
	
	public Row() {
	}
}
