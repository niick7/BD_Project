package part1b;

public class Pair {
	private String key;
	private int value;
	
	public Pair() {
		this.key 	= "";
		this.value 	= 0;
	}

	public Pair(String key, int value) {
		this.key 	= key;
		this.value 	= value;
	}
	
	public String getKey() {
		return key;
	}
	
	public void setKey(String key) {
		this.key = key;
	}
	
	public int getValue() {
		return value;
	}
	
	public void setValue(int value) {
		this.value = value;
	}
	
	@Override
	public String toString() {
		return String.format("<%s, %d>", key, value);
	}
	
}

