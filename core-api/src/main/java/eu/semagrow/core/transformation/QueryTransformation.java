package eu.semagrow.core.transformation;

import java.util.Collection;

import org.openrdf.model.URI;

/**
 * Query Transformation
 * 
 * @author Antonis Kukurikos
 */

public interface QueryTransformation
{

	/**
	 * @return A list of equivalent URIs aligned with a certain confidence
	 * with the initial URI and belonging to a specific schema
	 */
	public Collection<EquivalentURI> retrieveEquivalentURIs( URI uri );

    public URI getURI(URI source, int transformationID);

    public URI getInvURI(URI target, int transformationID);
}