package eu.semagrow.stack.modules.utils.patterndiscovery;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.openrdf.model.URI;

import eu.semagrow.stack.modules.api.EquivalentURI;

/**
 * For a {@link URI} get equivalent URIs aligned with a certain confidence with the initial URI and belonging to a specific schema.
 * 
 * @author Antonis Koukourikos
 *
 */
public interface PatternDiscovery {

	/**
	 * @return A {@link List} of {@link EquivalentURI}s.
	 */
	public List<EquivalentURI> retrieveEquivalentPatterns()
			throws IOException, ClassNotFoundException, SQLException;

}