/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package eu.semagrow.stack.modules.utils.endpoint.impl;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import eu.semagrow.stack.modules.utils.ReactivityParameters;
import eu.semagrow.stack.modules.utils.endpoint.SPARQLEndpoint;
import eu.semagrow.stack.modules.utils.federationWrapper.FederationEndpointWrapperComponent;
import eu.semagrow.stack.modules.utils.federationWrapper.impl.FederationEndpointWrapperComponentImpl;
import eu.semagrow.stack.modules.utils.queryDecomposition.DataSourceSelector;
import eu.semagrow.stack.modules.utils.queryDecomposition.DistributedExecutionPlan;
import eu.semagrow.stack.modules.utils.queryDecomposition.QueryDecompositionComponent;
import eu.semagrow.stack.modules.utils.queryDecomposition.impl.DataSourceSelectorImpl;
import eu.semagrow.stack.modules.utils.queryDecomposition.impl.QueryDecompositionComponentImpl;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryResult;
import org.openrdf.query.TupleQueryResultHandler;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.repository.http.HTTPRepository;

/**
 *
 * @author ggianna
 */
public class SPARQLEndpointImpl implements HttpHandler, SPARQLEndpoint,
        TupleQueryResultHandler {
    protected Map<UUID, HttpExchange> toServe;
    protected HttpServer server;
    protected HTTPRepository federationRepos;
    
    protected ReactivityParameters reactivityParameters;
    
    /**
     * Constants
     */
    public static final String STRATEGY_PARAM_NAME = "strategy";
    public static final String TIMEOUT_PARAM_NAME = "timeout";
    public static final String QUERY_PARAM_NAME = "query";
    public static final String SPARQL_URL_SUFFIX = "/sparql";
    
    
    public SPARQLEndpointImpl() {
        // Init query map
        toServe = new HashMap<UUID, HttpExchange>();
    }

    public void stopServing() {
        if (server != null)
            // TODO: Use parameter for delay seconds
            server.stop(5);
    }

    public void setFederationRepos(HTTPRepository federationRepos) {
        this.federationRepos = federationRepos;
    }

    
    public String getBaseURI() {
        // TODO: Change based on params
        return "http://localhost:18000" + SPARQL_URL_SUFFIX;
    }
    
//    /**
//     * Returns result to the client that called.
//     * @param res The query result returned.
//     */
//    public void useQueryResult(UUID queryID, QueryResult<BindingSet> res) {
//        System.err.println("Result:" + res.toString());
//    }

    public void handle(HttpExchange he) throws IOException {
        Map<String, Object> urlQueryParams;
        urlQueryParams = parseRequestQuery(
                he.getRequestURI().getQuery());
        String sQuery = urlQueryParams.get(QUERY_PARAM_NAME).toString();
        
        // Create unique ID (based on query string)
        UUID queryID;
        try {
            queryID = UUID.nameUUIDFromBytes(
                    he.getRequestURI().getRawQuery().toString().getBytes());
        } catch (Exception e) {
            Logger.getLogger(SPARQLEndpointImpl.class.getName()).log(
                    Level.WARNING, null, e);
            // Create random
            queryID = UUID.randomUUID();
        }
        
        // Map exchange to id
        toServe.put(queryID, he);
        
        // Get reactivity parameters
        // TODO: replace with constant parameter name
        String sStrategy = urlQueryParams.get(STRATEGY_PARAM_NAME).toString();
        int iTimeout = Integer.valueOf(urlQueryParams.get(TIMEOUT_PARAM_NAME).toString());
        
        try {
            ReactivityParameters rpParams = new ReactivityParameters(iTimeout, 
                    sStrategy);
            // Update reactivity params
            setReactivityParameters(rpParams);
            
            // Init federation endpoint wrapper component
            // TODO: Use param-based federation
            FederationEndpointWrapperComponent fed = 
                    new FederationEndpointWrapperComponentImpl(
                            new HTTPRepository(
                            "http://localhost:8080/openrdf-sesame/repositories/10")
                        );
            
            // Init decomposition component
            QueryDecompositionComponent qd = new QueryDecompositionComponentImpl(
                this);
            // Init datasource selector
            Iterator<DistributedExecutionPlan> idepPlans = qd.plansForQuery(queryID);
            // Initiate the execution of the query
            fed.executeDistributedQuery(this, sQuery, queryID, 
                    idepPlans, reactivityParameters);
        } catch (ReactivityParameters.InvalidStrategyException ex) {
            Logger.getLogger(SPARQLEndpointImpl.class.getName()).log(
                    Level.SEVERE, null, ex);
            // TODO: Return failure in a more efficient way
            new PrintStream(he.getResponseBody()).println(
                    "Cannot execute query.");
            he.getResponseBody().close();
        }
        
    }

    protected Map<String, Object> parseRequestQuery(String query) throws UnsupportedEncodingException 
    {
        Map<String, Object> parameters = new HashMap<String, Object>();

         if (query != null) {
             String pairs[] = query.split("[&]");

             for (String pair : pairs) {
                 String param[] = pair.split("[=]");

                 String key = null;
                 String value = null;
                 if (param.length > 0) {
                     key = URLDecoder.decode(param[0],
                         "utf8");
                 }

                 if (param.length > 1) {
                     value = URLDecoder.decode(param[1],
                         "utf8");
                 }

                 if (parameters.containsKey(key)) {
                     Object obj = parameters.get(key);
                     if(obj instanceof List<?>) {
                         List<String> values = (List<String>)obj;
                         values.add(value);
                     } else if(obj instanceof String) {
                         List<String> values = new ArrayList<String>();
                         values.add((String)obj);
                         values.add(value);
                         parameters.put(key, values);
                     }
                 } else {
                     parameters.put(key, value);
                 }
             }
         }
         return parameters;
    }
         
    public void setReactivityParameters(ReactivityParameters rpParams) {
        this.reactivityParameters = rpParams;
    }

    public void renderResults(UUID uQueryID, QueryResult<BindingSet> result) {
        PrintStream resultWriter = new PrintStream(
                toServe.get(uQueryID).getResponseBody());
        try {
            // For every result
            while (result.hasNext()) {
                BindingSet bsCur = result.next();
                Set<String> ssNames = bsCur.getBindingNames();
                
                // For each name in binding
                for (String sName: ssNames) {
                    // Output value
                    // TODO: Change
                    resultWriter.format("%s = %s", sName, 
                            bsCur.getValue(sName));
                }
            }
        } catch (QueryEvaluationException ex) {
            Logger.getLogger(SPARQLEndpointImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        try {
            //Finalize output
            toServe.get(uQueryID).getResponseBody().close();
        } catch (IOException ex) {
            Logger.getLogger(SPARQLEndpointImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void init() {
        try {
            // TODO: Read server settings from parameter file
            server = HttpServer.create(new InetSocketAddress(18000), 
                    0);
            server.createContext(SPARQL_URL_SUFFIX, this);
            Executor toUse = Executors.newFixedThreadPool(
                    Runtime.getRuntime().availableProcessors());
            server.setExecutor(toUse); // creates a default executor
            server.start();
        } catch (IOException ex) {
            Logger.getLogger(SPARQLEndpointImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        
    }

    public void cleanUp() {
        // TODO: Use a parameter for the shutdown delay
        this.server.stop(5);
    }

    // TODO: Implement one-by-one
    public void startQueryResult(List<String> list) throws TupleQueryResultHandlerException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public void endQueryResult() throws TupleQueryResultHandlerException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public void handleSolution(BindingSet bs) throws TupleQueryResultHandlerException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
