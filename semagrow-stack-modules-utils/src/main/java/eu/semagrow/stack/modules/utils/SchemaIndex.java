package eu.semagrow.stack.modules.utils;

import java.util.List;

import org.openrdf.model.URI;

import eu.semagrow.stack.modules.utils.SelectedResource;

/**
 * @author Giannis Mouchakis
 * @author Stasinos Konstantopoulos
 *
 */

public interface SchemaIndex {

	public List<SelectedResource> getEndpoints(URI uri);

}
