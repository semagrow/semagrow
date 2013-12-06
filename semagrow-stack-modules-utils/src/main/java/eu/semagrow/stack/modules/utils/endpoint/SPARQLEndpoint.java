/*
 * 
 */

package eu.semagrow.stack.modules.utils.endpoint;

import eu.semagrow.stack.modules.utils.ReactivityParameters;
import java.util.UUID;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryResult;

/**
 *
 * @author ggianna
 */
public interface SPARQLEndpoint {
    public void setReactivityParameters(ReactivityParameters rpParams);
    public void renderResults(UUID uQueryID, QueryResult<BindingSet> result);
    public String getBaseURI();
}
