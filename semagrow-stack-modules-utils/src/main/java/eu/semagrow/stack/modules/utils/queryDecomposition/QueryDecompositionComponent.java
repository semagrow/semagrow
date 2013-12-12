/*
 *
 */

package eu.semagrow.stack.modules.utils.queryDecomposition;

import eu.semagrow.stack.modules.utils.ReactivityParameters;
import eu.semagrow.stack.modules.utils.endpoint.SPARQLEndpoint;
import java.util.Iterator;
import java.util.UUID;

/**
 * The query decomposition component analyses SPARQL queries and decides about 
 * the optimal way to break them up into query fragments to be dispatched to 
 * sources’ endpoints. The query decomposition component comprises a decomposer 
 * module that syntactically analyses queries and suggests possible decompositions 
 * and a selector module that evaluates these suggestions using information and 
 * predictions from the resource discovery component about the data sources 
 * where each query fragment can be executed.
 * The result is a matching between query fragments and the source that each 
 * fragment is to be dispatched to. This is by necessity an approximation, 
 * since completeness can only be guaranteed by querying all sources. This is 
 * guided by the reactivity parameters that specify the client application’s 
 * wished position in the trade-off between efficiency and completeness in 
 * terms of how much time it is possible and worth it to wait to get more 
 * results, or the minimum number of results required, or some other similar 
 * policy balancing between completeness and effort.
 *
 * @author ggianna
 */
public interface QueryDecompositionComponent {
    public void decompose(SPARQLEndpoint caller, UUID uQueryID, String sQuery, 
            ReactivityParameters rpParams);
    public Iterator<DistributedExecutionPlan> plansForQuery(UUID queryID);
}
