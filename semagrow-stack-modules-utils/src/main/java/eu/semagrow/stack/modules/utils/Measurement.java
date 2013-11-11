/**
 * 
 */
package eu.semagrow.stack.modules.utils;

/**
 * @author Giannis Mouchakis
 *
 */
public class Measurement {
	
	private String type;
	private int value;
	
	/**
	 * @param type
	 * @param value
	 */
	public Measurement(String type, int value) {
		super();
		this.type = type;
		this.value = value;
	}

	/**
	 * @return the type
	 */
	public String getType() {
		return type;
	}

	/**
	 * @return the value
	 */
	public int getValue() {
		return value;
	}
	
	

}
