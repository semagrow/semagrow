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
import eu.semagrow.stack.modules.utils.queryDecomposition.QueryDecomposer;
import eu.semagrow.stack.modules.utils.queryDecomposition.impl.QueryDecomposerImpl;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryResult;
import org.openrdf.query.TupleQueryResultHandler;
import org.openrdf.query.TupleQueryResultHandlerException;

/**
 *
 * @author ggianna
 */
public class SPARQLEndpointImpl implements HttpHandler, SPARQLEndpoint,
        TupleQueryResultHandler {
    Map<UUID, HttpExchange> toServe;
    protected HttpServer server;
    
    protected ReactivityParameters reactivityParameters;
    /**
     * Constants
     */
    public static final String STRATEGY_PARAM_NAME = "strategy";
    public static final String TIMEOUT_PARAM_NAME = "timeout";
    public static final String QUERY_PARAM_NAME = "query";
    
    public SPARQLEndpointImpl() {
        // Init query map
        toServe = new HashMap<UUID, HttpExchange>();
    }

    public void stopServing() {
        if (server != null)
            // TODO: Use parameter for delay seconds
            server.stop(5);
    }
    
    public String getBaseURI() {
        // TODO: Change
        return "http://localhost:18000/";
    }
    
//    /**
//     * Returns result to the client that called.
//     * @param res The query result returned.
//     */
//    public void useQueryResult(UUID queryID, QueryResult<BindingSet> res) {
//        System.err.println("Result:" + res.toString());
//    }

    public void handle(HttpExchange he) throws IOException {
        // Create unique ID
        UUID queryID = UUID.fromString(he.getRequestBody().toString());
        // Map exchange to id
        toServe.put(queryID, he);
        // Call decomposer
        QueryDecomposer qd = new QueryDecomposerImpl();
        
        // Get reactivity parameters
        // TODO: replace with constant parameter name
        String sStrategy = he.getAttribute(STRATEGY_PARAM_NAME).toString();
        int iTimeout = Integer.valueOf(he.getAttribute(TIMEOUT_PARAM_NAME).toString());
        
        try {
            ReactivityParameters rpParams = new ReactivityParameters(iTimeout, 
                    sStrategy);
            // Update reactivity params
            setReactivityParameters(rpParams);
            
            // TODO: replace with constant parameter name
            qd.decomposeQuery(this, queryID, he.getAttribute("query").toString());
        } catch (ReactivityParameters.InvalidStrategyException ex) {
            Logger.getLogger(SPARQLEndpointImpl.class.getName()).log(
                    Level.SEVERE, null, ex);
            // TODO: Return failure in a more efficient way
            new PrintStream(he.getResponseBody()).println(
                    "Cannot execute query.");
            he.getResponseBody().close();
        } catch (MalformedQueryException ex) {
            Logger.getLogger(SPARQLEndpointImpl.class.getName()).log(
                    Level.SEVERE, null, ex);
            // Remove from pending
            toServe.remove(queryID);
            // TODO: Return failure in a more efficient way
            new PrintStream(he.getResponseBody()).println(
                    "Cannot execute query.");
            he.getResponseBody().close();
        } 
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
            server.createContext("/sparql", this);
            server.setExecutor(null); // creates a default executor
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
