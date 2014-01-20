/*
 * 
 */

package eu.semagrow.stack.modules.utils.federationWrapper.impl;

import eu.semagrow.stack.modules.utils.ReactivityParameters;
import eu.semagrow.stack.modules.utils.endpoint.SPARQLEndpoint;
import eu.semagrow.stack.modules.utils.federationWrapper.FederationEndpointWrapperComponent;
import eu.semagrow.stack.modules.utils.queryDecomposition.AlternativeDecomposition;
import eu.semagrow.stack.modules.utils.queryDecomposition.RemoteQueryFragment;
import eu.semagrow.stack.modules.utils.querytransformation.QueryTranformation;
import info.aduna.iteration.CloseableIteration;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;

import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.QueryResultUtil;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.impl.ListBindingSet;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.http.HTTPRepository;

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
//                List<Endpoint> lEndpoints = new ArrayList<Endpoint>();
                RepositoryConnection rc = null;
                
                // Init results list
                Map<RemoteQueryFragment, List<BindingSet>> fragsToRes = 
                        new HashMap<RemoteQueryFragment, List<BindingSet>>();
                
                for (RemoteQueryFragment uCurRFrag : curPlan.getRemoteQueryFragments()) {
                    
                    // DEBUG LINES
                    Logger.getLogger(FederationEndpointWrapperComponentImpl.class.getName()
                              ).info("Handling fragment:" + uCurRFrag.getFragment().getSourceString());
                    
                    // Init fragment results
                    fragsToRes.put(uCurRFrag, new ArrayList<BindingSet>());
                    
                    for (URI uCurSource : uCurRFrag.getSources()) {
                        try {
                            // Perform query
                            // DEBUG LINES
                            Logger.getLogger(FederationEndpointWrapperComponentImpl.class.getName()
                              ).info(
                              "-- Asking:" + uCurSource.toString());
                            
                            HTTPRepository sr = new HTTPRepository(uCurSource.toString());
//                            SPARQLRepository sr = new SPARQLRepository(uCurSource.toString());
                            sr.initialize();
                            ParsedQuery pqCur = (ParsedQuery)uCurRFrag.getFragment();
                            String sCurEquivQ = uCurRFrag.getFragment().getSourceString();

                            // TODO: Check type of query
                            rc = sr.getConnection();
                            TupleQuery tqCur = (TupleQuery)rc.prepareQuery(
                                            QueryLanguage.SPARQL, sCurEquivQ);
                            // TODO: Implement normally
                            bsRes = (CloseableIteration<BindingSet, QueryEvaluationException>) 
                                    tqCur.evaluate();
                            // Update results' list, if something was returned                            
                            if (bsRes != null)
                                if (bsRes.hasNext())
                                    fragsToRes.get(uCurRFrag).add(bsRes.next());
                            
                            // DEBUG LINES
                            Logger.getLogger(FederationEndpointWrapperComponentImpl.class.getName()
                              ).info("-- Got results.");
                            
                            /////////////
                        } catch (QueryEvaluationException ex) {
                            Logger.getLogger(FederationEndpointWrapperComponentImpl.class.getName()
                            ).log(Level.SEVERE, null, ex);
                        } catch (MalformedQueryException ex) {
                            Logger.getLogger(FederationEndpointWrapperComponentImpl.class.getName()).log(Level.SEVERE, null, ex);
                        }
                        finally {
                            if (rc != null)
                                rc.close();
                        }
                        
    //                    for (URI uCurEndpoint : uCurRFrag.getSources()) {
    //                        Endpoint e = new Endpoint(
    ////                                UUID.nameUUIDFromBytes(uCurEndpoint.toString().getBytes()).toString(), 
    //                                uCurEndpoint.toString(),
    //                                uCurEndpoint.getHost(), 
    //                                uCurEndpoint.toString(), 
    //                                Endpoint.EndpointType.SparqlEndpoint, 
    //                                Endpoint.EndpointClassification.Remote);
    //                        lEndpoints.add(e);
    //                    }
                    } // end for every source

                    // TODO: DEMO Perform full query
    //                // Initialize federation with defaults
    //                Config.initialize(null);
    //                SailRepository srFederation = 
    //                        FedXFactory.initializeFederation(lEndpoints);
    //                RepositoryConnection rc = srFederation.getConnection();
    //                SailRepositoryConnection sl = srFederation.getConnection();
    //                SailConnection sc = sl.getSailConnection();
                }
                // Join all results
                List<BindingSet> lbsRes = join(fragsToRes);
                
                // Perform filtering
                lbsRes = filter(lbsRes);

                
//                // Check if results were returned
                if (lbsRes != null) {
                    // Render results
                    caller.renderResults(queryID,
                            lbsRes);
                    break; // Ignore other plans on success
                }
                
            } // end while more plans exist
                        
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
//        catch (FedXException ex) {
//            Logger.getLogger(FederationEndpointWrapperComponentImpl.class.getName()
//            ).log(Level.SEVERE, "FedX federation failed...", ex);
//        }
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

    private List<BindingSet> join(Map<RemoteQueryFragment, List<BindingSet>> mFragRes) {
        List<BindingSet> lPreviousBindings = new ArrayList<BindingSet>();
        Iterator<RemoteQueryFragment> iCurPos = mFragRes.keySet().iterator();
        
        // For every binding set related to the current fragment
        while (iCurPos.hasNext()) {
            // Init new bindings list
            List<BindingSet> lNewBindings = new ArrayList<BindingSet>();
            // Get current step bindings, by moving iterator on
            List<BindingSet> lRightHandSide = mFragRes.get(iCurPos.next());
            // Assign on first run
            if (lPreviousBindings.isEmpty())
                lNewBindings.addAll(lRightHandSide);
            else
                // For every previous binding
                for (BindingSet bsLeftSideCur : lPreviousBindings) {
                    // For every new possible binding
                    for (BindingSet bsRightSideCur : lRightHandSide) {
                        List<String> lNewNames = new ArrayList<String>();
                        List<Value> lNewValues = new ArrayList<Value>();
                        // If OK
                        if (isCompatible(bsLeftSideCur, bsRightSideCur)) {
                            // For each var on left side
                            for (String sVarName : bsLeftSideCur.getBindingNames()) {
                                // If var is bound
                                if (bsLeftSideCur.hasBinding(sVarName)) {
                                    // just add
                                    // name
                                    lNewNames.add(sVarName);
                                    // and value
                                    lNewValues.add(bsLeftSideCur.getValue(sVarName));
                                }
                                else // if unbound
                                {
                                    // name
                                    lNewNames.add(sVarName);
                                    // if right side contains var
                                    if (bsRightSideCur.hasBinding(sVarName))
                                        // Assign existing value
                                        lNewValues.add(bsRightSideCur.getValue(sVarName));
                                    else
                                        // Assign null value (unbound)
                                        lNewValues.add(null);
                                }
                            }
                            
                            // For each var on right side
                            for (String sVarName : bsRightSideCur.getBindingNames()) {
                                // If not already added
                                if (!lNewNames.contains(sVarName)) {
                                    // If var is bound
                                    if (bsRightSideCur.hasBinding(sVarName)) {
                                        // just add
                                        // name
                                        lNewNames.add(sVarName);
                                        // and value
                                        lNewValues.add(bsRightSideCur.getValue(sVarName));
                                    }
                                    else // if unbound
                                    {
                                        // name
                                        lNewNames.add(sVarName);
                                        // Assign null value (unbound)
                                        lNewValues.add(null);
                                    }                                    
                                }
                            }
                            
                        } // end if
                        
                        // Create new binding
                        BindingSet bsNew = new ListBindingSet(lNewNames, lNewValues);
                        // Add to list
                        lNewBindings.add(bsNew);
                    }
                }
            // Update left hand side
            lPreviousBindings = lNewBindings;
        }
        
        // Return all results
        return lPreviousBindings;
    }
    
    public void setTransformationService(QueryTranformation transformer) {
        transformationService = transformer;
    }

    private boolean isCompatible(BindingSet bsLeftSideCur, BindingSet bsRightSideCur) {
        // Use existing implementation
        return QueryResultUtil.bindingSetsCompatible(bsLeftSideCur, 
                bsRightSideCur);

//        // For every variable
//        for (String sVarName: bsLeftSideCur.getBindingNames()) {
//            boolean bLeftBound = bsLeftSideCur.hasBinding(sVarName);
//            boolean bRightBound = bsRightSideCur.hasBinding(sVarName);
//            // If both bound
//            if (bLeftBound && bRightBound)
//                // but different value
//                if (!bsLeftSideCur.getValue(sVarName).equals(bsRightSideCur.getValue(sVarName)))
//                    // not compatible
//                    return false;
//        }
//        // else compatible
//        return true;
    }

    private List<BindingSet> filter(List<BindingSet> lbsRes) {
        // Filter
        /*
        ( ?M > 10 ) && 
        ( ( ?PRE - ?PRE2 ) < 1 ) ) && 
        ( ( ?PRE - ?PRE2 ) > -1 ) ) && 
        ( ( ?ALAT - ?ALAT2 ) < 1 ) ) && 
        ( ( ?ALAT - ?ALAT2 ) > -1 ) ) && 
        ( ?Longitude < 42.02 ) ) && 
        ( ?Latitude < 1.667 )        
        */
        
        List<BindingSet> lRes = new ArrayList<BindingSet>();
        
        for (BindingSet bsCur : lbsRes) {
            
            float M = Float.parseFloat(bsCur.getValue("M").stringValue());
            float  PRE = Float.parseFloat(bsCur.getValue("PRE").stringValue());
            float  PRE2 = Float.parseFloat(bsCur.getValue("PRE2").stringValue());
            float  Longitude = Float.parseFloat(bsCur.getValue("Longitude").stringValue());
            float  Latitude = Float.parseFloat(bsCur.getValue("Latitude").stringValue());
            // TODO: Check
//            float  ALAT2 = Float.parseFloat(bsCur.getValue("ALAT2").stringValue());
//            float  ALAT = Float.parseFloat(bsCur.getValue("ALAT").stringValue());
            float  ALAT2 = 0.0f;
            float  ALAT = 0.0f;

            if ( (M > 10) && 
                ( ( PRE - PRE2 ) < 1 )  && 
                ( ( PRE - PRE2 ) > -1 )  && 
                ( ( ALAT - ALAT2 ) < 1 )  && 
                ( ( ALAT - ALAT2 ) > -1 )  && 
                ( Longitude < 42.02 )  && 
                ( Latitude < 1.667 ) )
                // Add current
                lRes.add(bsCur);
                
        }
        
        return lRes;
    }
    
}
