/**
 * 
 */
package eu.semagrow.stack.modules.utils;

import java.net.URI;


/**
 * @author Giannis Mouchakis
 *
 */
public class EquivalentURI {
	
	private URI equivalent_URI;
	private int proximity;
	private URI schema;
	/**
	 * @param equivalent_URI
	 * @param proximity
	 * @param schema
	 */
	public EquivalentURI(URI equivalent_URI, int proximity, URI schema) {
		super();
		this.equivalent_URI = equivalent_URI;
		this.proximity = proximity;
		this.schema = schema;
	}
	/**
	 * @return the equivalent_URI
	 */
	public URI getEquivalent_URI() {
		return equivalent_URI;
	}
	/**
	 * @param equivalent_URI the equivalent_URI to set
	 */
	public void setEquivalent_URI(URI equivalent_URI) {
		this.equivalent_URI = equivalent_URI;
	}
	/**
	 * @return the proximity
	 */
	public int getProximity() {
		return proximity;
	}
	/**
	 * @param proximity the proximity to set
	 */
	public void setProximity(int proximity) {
		this.proximity = proximity;
	}
	/**
	 * @return the schema
	 */
	public URI getSchema() {
		return schema;
	}
	/**
	 * @param schema the schema to set
	 */
	public void setSchema(URI schema) {
		this.schema = schema;
	}
	
	


}
