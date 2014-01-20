/*
 * 
 */

package eu.semagrow.stack.modules.utils.federationWrapper.impl;

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
import java.util.HashMap;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;

import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.QueryResult;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.http.HTTPRepository;
import org.openrdf.repository.sparql.SPARQLRepository;

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
                RepositoryConnection rc = null;
                
                // Init results list
                Map<RemoteQueryFragment, List<BindingSet>> fragsToRes = 
                        new HashMap<RemoteQueryFragment, List<BindingSet>>();
                
                for (RemoteQueryFragment uCurRFrag : curPlan.getRemoteQueryFragments()) {
                    
                    // DEBUG LINES
                    Logger.getGlobal().info("Handling fragment:" + uCurRFrag.getFragment().getSourceString());
                    
                    // Init fragment results
                    fragsToRes.put(uCurRFrag, new ArrayList<BindingSet>());
                    
                    for (URI uCurSource : uCurRFrag.getSources()) {
                        try {
                            // Perform query
                            // DEBUG LINES
                            Logger.getGlobal().info(
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
                            Logger.getGlobal().info(
                                    "-- Got results.");
                            
                            /////////////
                        } catch (QueryEvaluationException ex) {
                            Logger.getLogger(FederationEndpointWrapperComponentImpl.class.getName()).log(Level.SEVERE, null, ex);
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
String sFullQueryWService = ""
        + "PREFIX  farm: <http://ontologies.seamless-ip.org/farm.owl#>\n" +
"PREFIX  dc:   <http://purl.org/dc/terms/>\n" +
"PREFIX  wgs84_pos: <http://www.w3.org/2003/01/geo/wgs84_pos#>\n" +
"PREFIX  t4f:  <http://semagrow.eu/schemas/t4f#>\n" +
"PREFIX  laflor: <http://semagrow.eu/schemas/laflor#>\n" +
"PREFIX  eururalis: <http://semagrow.eu/schemas/eururalis#>\n" +
"PREFIX  crop: <http://ontologies.seamless-ip.org/crop.owl#>\n" +
"\n" +
"SELECT  ?Longitude ?Latitude ?U\n" +
"WHERE\n" +
"  { " +
       "SERVICE <> " 
        + "{ SELECT  ?Longitude ?Latitude (avg(?PR) AS ?PRE)\n" +
"      WHERE\n" +
"        { ?R t4f:hasLong ?Longitude .\n" +
"          ?R t4f:hasLat ?Latitude .\n" +
"          ?R eururalis:landuse \"11\" .\n" +
"          ?R t4f:precipitation ?PR\n" +
"        }\n" +
"      GROUP BY ?Longitude ?Latitude\n" +
"    }\n" +
"    ?F farm:year 2010 .\n" +
"    ?F farm:cropinformation ?C .\n" +
"    ?C crop:productionmushrooms ?M .\n" +
"    ?F farm:agrienvironmentalzone ?A .\n" +
"    ?A farm:longitude ?ALONG .\n" +
"    ?A farm:latitude ?ALAT\n" +
"    { SELECT  ?A (avg(?PR) AS ?PRE2)\n" +
"      WHERE\n" +
"        { ?A farm:dailyclimate ?D .\n" +
"          ?D farm:rainfall ?PR\n" +
"        }\n" +
"      GROUP BY ?A\n" +
"    }\n" +
"    ?J dc:subject <http://aims.fao.org/aos/agrovoc/xl_en_1299487055215> .\n" +
"    ?J laflor:location ?U .\n" +
"    ?J laflor:language <http://id.loc.gov/vocabulary/iso639-2/es> .\n" +
"    ?J <http://schema.org/about> ?P .\n" +
"    ?P wgs84_pos:lat ?ALAT2 .\n" +
"    ?P wgs84_pos:long ?ALONG2\n" +
"    FILTER ( ( ( ( ( ( ( ?M > 10 ) && ( ( ?PRE - ?PRE2 ) < 1 ) ) && ( ( ?PRE - ?PRE2 ) > -1 ) ) && ( ( ?ALAT - ?ALAT2 ) < 1 ) ) && ( ( ?ALAT - ?ALAT2 ) > -1 ) ) && ( ?Longitude < 42.02 ) ) && ( ?Latitude < 1.667 ) )\n" +
"  }";                    
                    
    //                // Initialize federation with defaults
    //                Config.initialize(null);
    //                SailRepository srFederation = 
    //                        FedXFactory.initializeFederation(lEndpoints);
    //                RepositoryConnection rc = srFederation.getConnection();
    //                SailRepositoryConnection sl = srFederation.getConnection();
    //                SailConnection sc = sl.getSailConnection();
                }
                
                
                // Check if results were returned
                if (bsRes != null) {
                    // Render results
                    caller.renderResults(queryID, (
                            QueryResult<BindingSet>) bsRes);
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

    public void setTransformationService(QueryTranformation transformer) {
        transformationService = transformer;
    }
    
}
