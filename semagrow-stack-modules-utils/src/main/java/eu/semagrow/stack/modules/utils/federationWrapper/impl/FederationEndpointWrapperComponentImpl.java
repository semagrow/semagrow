/*
 * 
 */

package eu.semagrow.stack.modules.utils.federationWrapper.impl;

import com.fluidops.fedx.FedXFactory;
import com.fluidops.fedx.exception.FedXException;
import com.fluidops.fedx.structures.Endpoint;
import eu.semagrow.stack.modules.utils.ReactivityParameters;
import eu.semagrow.stack.modules.utils.endpoint.SPARQLEndpoint;
import eu.semagrow.stack.modules.utils.federationWrapper.FederationEndpointWrapperComponent;
import eu.semagrow.stack.modules.utils.queryDecomposition.AlternativeDecomposition;
import eu.semagrow.stack.modules.utils.querytransformation.QueryTranformation;
import java.net.URI;
import java.util.ArrayList;

import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.QueryParserUtil;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;

/**
 *
 * @author ggianna
 */
public class FederationEndpointWrapperComponentImpl implements 
        FederationEndpointWrapperComponent {

    SPARQLEndpoint caller;
    String query;
    Iterator<AlternativeDecomposition> possibleDecompositions;
    QueryTranformation transformationService;

    public FederationEndpointWrapperComponentImpl() 
    {
    }

    
    
    public void executeDistributedQuery(SPARQLEndpoint caller, String query, 
            UUID queryID,
            Iterator<AlternativeDecomposition> possiblePlans, 
            ReactivityParameters rpParams) {
        this.caller = caller;
        this.query = query;
        this.possibleDecompositions = possiblePlans;
        
        
        ParsedQuery p = null;
        TupleQuery q = null;
        TupleQueryResult t = null;
        
        try {
            // Check query
            p = QueryParserUtil.parseQuery(QueryLanguage.SPARQL, 
                    query, "http://semagrow.eu/");
            
            
            // Initialize connection
            
            // For every plan
            while (possiblePlans.hasNext()) {
                // Get all endpoints in plan
                AlternativeDecomposition curPlan = possiblePlans.next();
                // Initialize list
                List<Endpoint> lEndpoints = new ArrayList<Endpoint>();
                for (URI uCurEndpointURI : curPlan.getEndpoints()) {
                    Endpoint e = new Endpoint(uCurEndpointURI.toString(), 
                            uCurEndpointURI.getHost(), 
                            uCurEndpointURI.toString(), 
                            Endpoint.EndpointType.SparqlEndpoint, 
                            Endpoint.EndpointClassification.Remote);
                    lEndpoints.add(e);
                }
                    
                // Initialize federation
                SailRepository srFederation = 
                        FedXFactory.initializeFederation(lEndpoints);
                RepositoryConnection rc = srFederation.getConnection();
                
                // Perform query
                q = rc.prepareTupleQuery(QueryLanguage.SPARQL, query);
                // Set the time, taking into account the time limitations
                q.setMaxQueryTime(rpParams.getMaximumResponseTime());

                if (rpParams.getStrategy().equals(
                        ReactivityParameters.STRATEGY_DELIVER_ON_ARRIVAL))
                    // Call incrementally
                    try {
                        // Call incrementally
                        q.evaluate(caller);
                        
                    } catch (QueryEvaluationException ex) {
                        Logger.getLogger(
                          FederationEndpointWrapperComponentImpl.class.getName()
                        ).log(Level.SEVERE, "Could not incrementally "
                                + "evaluate query..."
                                + " Trying alternate plan.", ex);
                    }
                else
                {
                    try {
                        // Call one-put
                        t = q.evaluate();
                        break; // Ignore other plans
                    } catch (QueryEvaluationException ex) {
                        Logger.getLogger(
                          FederationEndpointWrapperComponentImpl.class.getName()
                        ).log(Level.SEVERE, "Could not evaluate query..."
                                + " Trying alternate plan.", ex);
                    }
                }
                // Render results
                caller.renderResults(queryID, t);
                
            }
                        
        } catch (RepositoryException ex) {
            Logger.getLogger(
                    FederationEndpointWrapperComponentImpl.class.getName()).log(
                            Level.SEVERE, null, ex);
        } catch (MalformedQueryException ex) {
            p = null;
            Logger.getLogger(
                    FederationEndpointWrapperComponentImpl.class.getName()).log(
                            Level.SEVERE, null, ex);
        } catch (TupleQueryResultHandlerException ex) {
            Logger.getLogger(FederationEndpointWrapperComponentImpl.class.getName()).log(Level.SEVERE, null, ex);
        } catch (FedXException ex) {
            Logger.getLogger(FederationEndpointWrapperComponentImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally {
            // TODO: Handle malformed query
            // Clean-up query
            if (t != null) 
            {
                try {
                        t.close();
                        t = null;
                } catch (QueryEvaluationException ex) {
                    Logger.getLogger(
                            FederationEndpointWrapperComponentImpl.class.getName()
                            ).log(Level.SEVERE, null, ex);
                }
            }
        }
    }

    public void setTransformationService(QueryTranformation transformer) {
        transformationService = transformer;
    }
    
}
