/*
 * 
 */

package eu.semagrow.stack.modules.utils.federationWrapper;

import eu.semagrow.stack.modules.utils.ReactivityParameters;
import eu.semagrow.stack.modules.utils.endpoint.SPARQLEndpoint;
import eu.semagrow.stack.modules.utils.queryDecomposition.AlternativeDecomposition;
import eu.semagrow.stack.modules.api.transformation.QueryTranformation;

import java.util.Iterator;
import java.util.UUID;
import org.openrdf.query.parser.ParsedQuery;

/**
 * The Federated End-point Wrapper manages the communication with the external 
 * data sources that are federated by the SemaGrow Stack. Its Query Manager 
 * module is responsible for (a) where necessary, applying the Query 
 * Transformation Service to access repositories that follow a different schema 
 * than the one of the original query; (b) forwarding query fragments to the 
 * Query Results Merger; and (c) collecting and forwarding dynamic run-time 
 * statistics to the Resource Discovery components. 
 * The Query Transformation Service applies alignment knowledge served from the
 * schema mappings repository. It re-writes query fragments from the schema of
 * the original query to that of the data source that will be used for each
 * fragment and also query results back into the schema of the original query
 * so that they can be joined with results from other sources.
 * As joining distributed query results can degenerate into a situation where 
 * massive data volumes need to be copied to and processed by the results 
 * collector, SemaGrow envisages a Query Results Merger that exhibits 
 * pay-as-you-go behaviour, providing a first approximation with minimal usage 
 * of computational resources and iteratively refining it if more computation 
 * time and space are warranted by the reactivity parameters set by the client 
 * application. Distributed incremental result fetching operators exhibit this 
 * property, so that results can be incrementally requested and forwarded on 
 * arrival of new tuples. The confidence of mappings can be taken into account,
 * offering higher priority to more certain mappings when needed, as per user 
 * requirements.
 * @author ggianna
 */
public interface FederationEndpointWrapperComponent {
    /**
     * Executes a query requested from a given endpoint, based on a set
     * of alternative plans.
     * @param endpoint The SPARQLEndpoint that requested the query.
     * @param sQuery The original full text query.
     * @param queryID The UUID of the query.
     * @param decompositions An iterator of possible decompositions to execute.
     * @param rpParams The responsiveness parameters.
     */
    public void executeDistributedQuery(SPARQLEndpoint endpoint, ParsedQuery sQuery,
            UUID queryID, Iterator<AlternativeDecomposition> decompositions, 
            ReactivityParameters rpParams);
    public void setTransformationService(QueryTranformation transformer);
}
