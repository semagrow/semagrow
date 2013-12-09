package eu.semagrow.stack.modules.utils.resourceselector;

import org.openrdf.model.URI;

/**
 * For a URI this object holds its equivalent URI, the proximity of the
 * equivalent to the original URI and the schema URI that the equivalent URI
 * belongs to.
 * 
 * @author Giannis Mouchakis
 * 
 */
public interface EquivalentURI {

	/**
	 * @return the equivalent URI
	 */
	public URI getEquivalent_URI();

	/**
	 * @return the proximity to the original URI
	 */
	public int getProximity();

	/**
	 * @return the identifier of the RDF schema of the equivalent URI
	 */
	public URI getSchema();

}