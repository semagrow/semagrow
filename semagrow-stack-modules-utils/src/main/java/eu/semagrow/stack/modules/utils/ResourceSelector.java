package eu.semagrow.stack.modules.utils;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

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
	 * 
	 * Public method to run the ResourceSelector module.
	 * 
	 * @return A list of {@link SelectedResource} objects. Empty list if none found.
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 * @throws IOException
	 */
	public List<SelectedResource> getSelectedResources() throws SQLException,
			ClassNotFoundException, IOException;

}