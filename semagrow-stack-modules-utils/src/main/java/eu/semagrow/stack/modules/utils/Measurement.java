/**
 * 
 */
package eu.semagrow.stack.modules.utils;

/**
 * @author Giannis Mouchakis
 *
 */
public class Measurement {
	
	private int id;
	private String type;
	private int value;
	
	/**
	 * @param id
	 * @param type
	 * @param value
	 */
	public Measurement(int id, String type, int value) {
		super();
		this.id = id;
		this.type = type;
		this.value = value;
	}
	
	/**
	 * @return the id
	 */
	public int getId() {
		return id;
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
