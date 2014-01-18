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
import eu.semagrow.stack.modules.utils.queryDecomposition.RemoteQueryFragment;
import eu.semagrow.stack.modules.utils.querytransformation.QueryTranformation;
import info.aduna.iteration.CloseableIteration;
import java.net.URI;
import java.util.ArrayList;

import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.openrdf.query.BindingSet;

import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryResult;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

/**
 *
 * @author ggianna
 */
public class FederationEndpointWrapperComponentImpl implements 
        FederationEndpointWrapperComponent {

    SPARQLEndpoint caller;
    ParsedQuery query;
    Iterator<AlternativeDecomposition> possibleDecompositions;
    QueryTranformation transformationService;

    public FederationEndpointWrapperComponentImpl() 
    {
    }

    
    
    public void executeDistributedQuery(SPARQLEndpoint caller, ParsedQuery query, 
            UUID queryID,
            Iterator<AlternativeDecomposition> possiblePlans, 
            ReactivityParameters rpParams) {
        this.caller = caller;
        this.query = query;
        this.possibleDecompositions = possiblePlans;
        
        
//        TupleQuery q = null;
//        TupleQueryResult t = null;
        CloseableIteration<BindingSet, QueryEvaluationException> bsRes = null;
        
        try {
            // For every plan
            while (possiblePlans.hasNext()) {
                // Get all endpoints in plan
                AlternativeDecomposition curPlan = possiblePlans.next();
                // Initialize list
                List<Endpoint> lEndpoints = new ArrayList<Endpoint>();
                for (RemoteQueryFragment uCurRFrag : curPlan.getRemoteQueryFragments()) {
                    for (URI uCurEndpoint : uCurRFrag.getSources()) {
                        Endpoint e = new Endpoint(uCurEndpoint.toString(), 
                                uCurEndpoint.getHost(), 
                                uCurEndpoint.toString(), 
                                Endpoint.EndpointType.SparqlEndpoint, 
                                Endpoint.EndpointClassification.Remote);
                        lEndpoints.add(e);
                    }
                }
                    
                // Initialize federation
                SailRepository srFederation = 
                        FedXFactory.initializeFederation(lEndpoints);
                RepositoryConnection rc = srFederation.getConnection();
                SailRepositoryConnection sl = srFederation.getConnection();
                SailConnection sc = sl.getSailConnection();
                
                
                // BEGIN DEMO
                // Initialize binding set
                
                try {
                    // Perform query
                    // TODO: Implement normally
                    bsRes = (CloseableIteration<BindingSet, QueryEvaluationException>) 
                            sc.evaluate(query.getTupleExpr(), null, 
                                new EmptyBindingSet(), false);
                    /////////////
                } catch (SailException ex) {
                    Logger.getLogger(
                      FederationEndpointWrapperComponentImpl.class.getName()
                      ).log(Level.SEVERE, "Could not evaluate...", ex);
                    
                }
                // END DEMO
                try {
                    // TODO: Restore
//                // Set the time, taking into account the time limitations
//                q.setMaxQueryTime(rpParams.getMaximumResponseTime());

//                if (rpParams.getStrategy().equals(
//                        ReactivityParameters.STRATEGY_DELIVER_ON_ARRIVAL))
//                    // Call incrementally
//                    try {
//                        // Call incrementally
//                        q.evaluate(caller);
//                        
//                    } catch (QueryEvaluationException ex) {
//                        Logger.getLogger(
//                          FederationEndpointWrapperComponentImpl.class.getName()
//                        ).log(Level.SEVERE, "Could not incrementally "
//                                + "evaluate query..."
//                                + " Trying alternate plan.", ex);
//                    }
//                else
//                {
//                    try {
//                        // Call one-put
//                        t = q.evaluate();
//                        break; // Ignore other plans
//                    } catch (QueryEvaluationException ex) {
//                        Logger.getLogger(
//                          FederationEndpointWrapperComponentImpl.class.getName()
//                        ).log(Level.SEVERE, "Could not evaluate query..."
//                                + " Trying alternate plan.", ex);
//                    }
//                }
                    // Render results
                    caller.renderResults(queryID, (QueryResult<BindingSet>) bsRes.next());
                } catch (QueryEvaluationException ex) {
                    Logger.getLogger(FederationEndpointWrapperComponentImpl.class.getName()).log(
                            Level.SEVERE, null, ex);
                }
                
            }
                        
        } catch (RepositoryException ex) {
            Logger.getLogger(
                    FederationEndpointWrapperComponentImpl.class.getName()).log(
                            Level.SEVERE, null, ex);
        } 
//        catch (MalformedQueryException ex) {
//            p = null;
//            Logger.getLogger(
//                    FederationEndpointWrapperComponentImpl.class.getName()).log(
//                            Level.SEVERE, null, ex);
//        } catch (TupleQueryResultHandlerException ex) {
//            Logger.getLogger(FederationEndpointWrapperComponentImpl.class.getName()).log(Level.SEVERE, null, ex);
//        } 
        catch (FedXException ex) {
            Logger.getLogger(FederationEndpointWrapperComponentImpl.class.getName()
            ).log(Level.SEVERE, "FedX federation failed...", ex);
        }
        finally {
            // TODO: Handle malformed query
            // Clean-up query
            if (bsRes != null) 
            {
//                try {
//                        t.close();
//                        t = null;
//                } catch (QueryEvaluationException ex) {
//                    Logger.getLogger(
//                            FederationEndpointWrapperComponentImpl.class.getName()
//                            ).log(Level.SEVERE, null, ex);
//                }
            }
        }
    }

    public void setTransformationService(QueryTranformation transformer) {
        transformationService = transformer;
    }
    
}
