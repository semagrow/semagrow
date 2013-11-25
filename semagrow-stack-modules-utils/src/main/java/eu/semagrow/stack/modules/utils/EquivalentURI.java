/**
 * 
 */
package eu.semagrow.stack.modules.utils;

import java.net.URI;


/**
 * For a URI this object holds its equivalent URI, the proximity of the equivalent to the original URI and the schema URI.
 * 
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
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "EquivalentURI [equivalent_URI=" + equivalent_URI
				+ ", proximity=" + proximity + ", schema=" + schema + "]";
	}
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((equivalent_URI == null) ? 0 : equivalent_URI.hashCode());
		result = prime * result + proximity;
		result = prime * result + ((schema == null) ? 0 : schema.hashCode());
		return result;
	}
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		EquivalentURI other = (EquivalentURI) obj;
		if (equivalent_URI == null) {
			if (other.equivalent_URI != null)
				return false;
		} else if (!equivalent_URI.equals(other.equivalent_URI))
			return false;
		if (proximity != other.proximity)
			return false;
		if (schema == null) {
			if (other.schema != null)
				return false;
		} else if (!schema.equals(other.schema))
			return false;
		return true;
	}


}
