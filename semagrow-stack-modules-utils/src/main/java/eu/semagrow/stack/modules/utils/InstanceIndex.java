package eu.semagrow.stack.modules.utils;

import java.util.List;

import org.openrdf.model.URI;

/**
 * @author Giannis Mouchakis
 *
 */
public interface InstanceIndex {

	public List<SelectedResource> getEndpoints(URI uri);

}