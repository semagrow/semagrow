package eu.semagrow.stack.modules.utils;

/**
 * Used to hold load info of a source.
 * 
 * @author Giannis Mouchakis
 *
 */
public interface Measurement {

	/**
	 * @return the id
	 */
	public int getId();

	/**
	 * @return the type
	 */
	public String getType();

	/**
	 * @return the value
	 */
	public int getValue();

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString();

}