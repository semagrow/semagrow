/*
 * 
 */

package eu.semagrow.stack.modules.utils.endpoint;

import eu.semagrow.stack.modules.utils.ReactivityParameters;
import java.util.List;
import java.util.UUID;
import org.openrdf.query.BindingSet;
import org.openrdf.query.TupleQueryResultHandler;

/**
 *
 * @author ggianna
 */
public interface SPARQLEndpoint extends TupleQueryResultHandler{
    public void init();
    public void setReactivityParameters(ReactivityParameters rpParams);
    public void renderResults(UUID uQueryID, List<BindingSet> result);
    public String getBaseURI();    
    public void cleanUp();
}
