package eu.semagrow.stack.modules.utils.resourceselector;

import java.util.List;

import org.openrdf.model.URI;

/**
 * Instances of this class act as proxies for repositories
 * holding schema-level metadata about the data sources of
 * the federation.
 *
 * @author Stasinos Konstantopoulos
 */

public interface SchemaIndex {

	/**
	 * This method returns a {@link List} of {@link SelectedResource}
	 * instances, each of which indicate a data source and the volume
	 * of triples involving {@code uri} available at that data source
	 * 
	 * @return
	 */
	public List<SelectedResource> getEndpoints(URI uri);

}
