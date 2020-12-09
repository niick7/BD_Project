package part3;

public class StrIntPair {
	private String strVal;
	private int intVal;
	
	public StrIntPair() {
		this.strVal= "";
		this.intVal= 0;
	}

	public StrIntPair(String strVal, int intVal) {
		this.strVal = strVal;
		this.intVal = intVal;
	}
	
	public String getStrVal() {
		return strVal;
	}
	
	public void setStrVal(String strVal) {
		this.strVal = strVal;
	}
	
	public int getIntVal() {
		return intVal;
	}
	
	public void setIntVal(int intVal) {
		this.intVal = intVal;
	}

	@Override
	public String toString() {
		return String.format("<%s, %d>", strVal, intVal);
	}	
}
