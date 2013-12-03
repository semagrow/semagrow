/*
 * 
 */

package eu.semagrow.stack.modules.utils;

import org.openrdf.http.server.repository.RepositoryController;
import org.openrdf.repository.sparql.SPARQLRepository;

/**
 *
 * @author ggianna
 */
public class SemagrowSPARQLEndpoint extends RepositoryController {

    /** Initializes the Semagrow SPARQL endpoint
     * 
     * @param queryEndpointURL 
     */
    public SemagrowSPARQLEndpoint(String queryEndpointURL) {
        super(queryEndpointURL);
    }

    
}
