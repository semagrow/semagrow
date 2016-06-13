package eu.semagrow.core.transformation;

import java.util.Collection;

import org.eclipse.rdf4j.model.IRI;

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
	Collection<EquivalentURI> retrieveEquivalentURIs( IRI uri );

    IRI getURI(IRI source, int transformationID);

	IRI getInvURI(IRI target, int transformationID);
}