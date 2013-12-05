package eu.semagrow.stack.modules.utils;

/**
 * Used to hold load info of a source.
 * 
 * @author Giannis Mouchakis
 *
 */
public interface Measurement {

	/**
	 * @return the id of the measurement
	 */
	public int getId();

	/**
	 * @return the type of the measurement
	 */
	public String getType();

	/**
	 * @return the value of the measurement
	 */
	public int getValue();


}