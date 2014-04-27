package eu.semagrow.stack.modules.utils.resourceselector;

import java.util.List;

import eu.semagrow.stack.modules.api.SelectedResource;
import org.openrdf.model.URI;

/**
 * Instances of this class act as proxies for repositories
 * holding instance-level metadata about the data sources of
 * the federation.
 *
 * @author Stasinos Konstantopoulos
 */

public interface InstanceIndex {

	/**
	 * This method returns a {@link List} of {@link SelectedResource}
	 * instances, each of which indicate a data source and the volume
	 * of triples involving {@code uri} available at that data source
	 * 
	 * @return
	 */
	public List<SelectedResource> getEndpoints(URI uri);

}