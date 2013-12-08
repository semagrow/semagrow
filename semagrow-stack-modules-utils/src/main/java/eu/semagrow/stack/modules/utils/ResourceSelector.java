package eu.semagrow.stack.modules.utils;

import java.util.List;

import org.openrdf.query.algebra.StatementPattern;

/**
 * This component provides an annotated list of candidate data sources that
 * possibly hold triples matching a given query pattern; including sources that
 * follow a different (but aligned) schema than that of the query pattern. The
 * sources are annotated with schema and instance-level metadata and predicted
 * response volume from the data summaries endpoint; as well as run-time
 * information about current source load. When a source following an aligned
 * schema is used, the annotation also includes relevant meta-information, such
 * as the semantic proximity of the query schema and the source schema.
 * 
 * @author Giannis Mouchakis
 * 
 */
public interface ResourceSelector {

	/**
	 * Public method that acts as entry point to the ResourceSelector module.
	 * 
	 * @param statementPattern
	 *            the {@link StatementPattern} (query pattern) to examine.
	 * @param measurement_id
	 *            used to determine how many load info measurements should be
	 *            returned for each candidate source endpoint. Each measurement has an
	 *            incremental id so this method returns the load info of an
	 *            endpoint where id >= measurement_id
	 * 
	 * @return A list of {@link SelectedResource} objects. Empty list if no
	 *         resources found. Null in case of exceptions.
	 */
	public List<SelectedResource> getSelectedResources(StatementPattern statementPattern, int measurement_id);

}