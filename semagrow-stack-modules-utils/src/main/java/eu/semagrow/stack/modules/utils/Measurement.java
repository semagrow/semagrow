package eu.semagrow.stack.modules.utils;

/**
 * Used to hold the query results returned by the {@link ResourceSelector} getLoadInfo method (see method's javadoc).
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