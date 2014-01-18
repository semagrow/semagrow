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
import eu.semagrow.stack.modules.utils.queryDecomposition.AlternativeDecomposition;
import eu.semagrow.stack.modules.utils.queryDecomposition.QueryDecompositionComponent;
import eu.semagrow.stack.modules.utils.queryDecomposition.impl.QueryDecompositionComponentImpl;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
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
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.QueryResult;
import org.openrdf.query.TupleQueryResultHandler;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.UnsupportedQueryLanguageException;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.QueryParserUtil;
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
    protected int Port;
    
    protected ReactivityParameters reactivityParameters;
    protected URI FederationURI;
    
    
    /**
     * Constants
     */
    public static final String STRATEGY_PARAM_NAME = "strategy";
    public static final String TIMEOUT_PARAM_NAME = "timeout";
    public static final String QUERY_PARAM_NAME = "query";
    public static final String SPARQL_URL_SUFFIX = "/sparql";
    public static final String SERVER_STOP_SUFFIX = "/sparql/quit";
    
    public SPARQLEndpointImpl(String sFederationURL) throws URISyntaxException {
        // Init query map
        toServe = new HashMap<UUID, HttpExchange>();
        // Initialize the URI and serving port
        FederationURI = new URI(sFederationURL);
        Port = FederationURI.getPort();
    }

    public void stopServing() {
        if (server != null)
            // TODO: Use parameter for delay seconds
            server.stop(5);
    }

    /**
     * 
     * @param federationRepos 
     */
    public void setFederationRepos(HTTPRepository federationRepos) {
        this.federationRepos = federationRepos;
    }


    /**
     * Returns the URI of the server endpoint.
     * @return The URI as a string.
     */
    public String getBaseURI() {
//        return "http://" + server.getAddress().getHostString() + ":" +
//                String.valueOf(server.getAddress().getPort()) + 
//                SPARQL_URL_SUFFIX;
        return FederationURI + SPARQL_URL_SUFFIX;
    }
    
//    /**
//     * Returns result to the client that called.
//     * @param res The query result returned.
//     */
//    public void useQueryResult(UUID queryID, QueryResult<BindingSet> res) {
//        System.err.println("Result:" + res.toString());
//    }

    public void handle(HttpExchange he) throws IOException {
        // Get parameters
        Map<String, Object> urlQueryParams;
        urlQueryParams = parseRequestQuery(
                he.getRequestURI().getQuery());
        String sQuery = urlQueryParams.get(QUERY_PARAM_NAME).toString();
        
        // Try parsing the query
        ParsedQuery pqQuery = null;
        try {
            pqQuery = QueryParserUtil.parseQuery(QueryLanguage.SPARQL, 
                    sQuery, "http://semagrow.eu/");
        } catch (MalformedQueryException ex) {
            Logger.getLogger(SPARQLEndpointImpl.class.getName()).log(Level.WARNING
                    , "Malformed query:\n" + sQuery,  ex);
            
            returnError(he, "Malformed query:\n" + sQuery + "\n" + 
                    ex.getMessage());
            return;
        } catch (UnsupportedQueryLanguageException ex) {
            Logger.getLogger(SPARQLEndpointImpl.class.getName()).log(Level.WARNING
                    , "Unsupported query language. Should NOT happen if SPARQL is used."
                            + "Please check:\n" + sQuery,  ex);
            returnError(he, "Server error. The server does not seem capable to "
                    + "handle SPARQL. Please contact the administrator.");
            return;
        }
        
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
        // TODO: Update as required
        String sStrategy = urlQueryParams.get(STRATEGY_PARAM_NAME).toString();
        int iTimeout = Integer.valueOf(urlQueryParams.get(TIMEOUT_PARAM_NAME).toString());
        
        try {
            ReactivityParameters rpParams = new ReactivityParameters(iTimeout, 
                    sStrategy);
            // Update reactivity params
            setReactivityParameters(rpParams);
            
            // Init decomposition component
            // TODO: Check if we need a static one to avoid excess objects
            final QueryDecompositionComponent qd = new QueryDecompositionComponentImpl(
                this);
            // Perform decomposition
            final Iterator<AlternativeDecomposition> idepDecompositions = 
                    qd.decompose(this, queryID, pqQuery, rpParams);
            
            
            // Handle NO PLANS case (i.e. impossible query)
            if (!idepDecompositions.hasNext()) {
                returnError(he, "No viable plans found. Aboring execution.");
                return;
            }
            
            // Init federation endpoint wrapper component
            final FederationEndpointWrapperComponent fed = 
                    new FederationEndpointWrapperComponentImpl();
            // Create multi-threaded arguments, as needed
            final SPARQLEndpoint replyTo = this;
            final ParsedQuery pqQueryArg = pqQuery;
            final UUID queryIDArg = queryID;            
            
            // Initiate the asynchronous execution of the query
            new Thread(new Runnable() {
                public void run() {
                    fed.executeDistributedQuery(replyTo, pqQueryArg, queryIDArg, 
                        idepDecompositions, reactivityParameters);
                }
            }).start();
            
        } catch (ReactivityParameters.InvalidStrategyException ex) {
            Logger.getLogger(SPARQLEndpointImpl.class.getName()).log(
                    Level.SEVERE, null, ex);
            
            returnError(he, "Invalid strategy requested.");
            return;
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
            // Init HTML output
            resultWriter.append("<HTML><BODY><PRE>");
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
            // Finalize HTML output
            resultWriter.append("</PRE></BODY></HTML>");
        } catch (QueryEvaluationException ex) {
            Logger.getLogger(SPARQLEndpointImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        try {
            // Type of response
            toServe.get(uQueryID).getResponseHeaders().add("Content-Type", "text/html; charset=UTF-8");
            // Send OK header together with other headers
            toServe.get(uQueryID).sendResponseHeaders(200, 0);
            //Finalize output
            toServe.get(uQueryID).getResponseBody().close();
        } catch (IOException ex) {
            Logger.getLogger(SPARQLEndpointImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void init() {
        try {
            // Init listener
            server = HttpServer.create(new InetSocketAddress(Port), 
                    0);
            // Add context handler
            server.createContext(FederationURI.getPath() + SPARQL_URL_SUFFIX, 
                    this);
            
            // Allow shutdown
            // TODO: Remove when not for test
            final SPARQLEndpointImpl me = this;
            server.createContext(SERVER_STOP_SUFFIX, new HttpHandler() {

                public void handle(HttpExchange he) throws IOException {
                    me.stopServing();
                }
            });
            
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

    private void returnError(HttpExchange he, String sErrorMsg) {
        PrintStream errorWriter = new PrintStream(
                he.getResponseBody());
        errorWriter.print("<html><body><h1>Error:</h1>\n<code>" + 
                sErrorMsg + "</code><body></html>");
        try {
            // Type of response
            he.getResponseHeaders().add("Content-Type", "text/html; charset=UTF-8");
            // Send OK header together with other headers
            he.sendResponseHeaders(200, 0);
            //Finalize output
            he.getResponseBody().close();
        } catch (IOException ex) {
            Logger.getLogger(SPARQLEndpointImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
