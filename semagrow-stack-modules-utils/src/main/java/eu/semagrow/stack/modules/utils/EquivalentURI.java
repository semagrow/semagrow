package eu.semagrow.stack.modules.utils;

import org.openrdf.model.URI;

/**
 * For a URI this object holds its equivalent URI, the proximity of the equivalent to the original URI and the schema URI.
 * 
 * @author Giannis Mouchakis
 *
 */
public interface EquivalentURI {

	/**
	 * @return the equivalent_URI
	 */
	public URI getEquivalent_URI();

	/**
	 * @param equivalent_URI the equivalent_URI to set
	 */
	public void setEquivalent_URI(URI equivalent_URI);

	/**
	 * @return the proximity
	 */
	public int getProximity();

	/**
	 * @param proximity the proximity to set
	 */
	public void setProximity(int proximity);

	/**
	 * @return the schema
	 */
	public URI getSchema();

	/**
	 * @param schema the schema to set
	 */
	public void setSchema(URI schema);

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString();

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode();

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	public boolean equals(Object obj);

}