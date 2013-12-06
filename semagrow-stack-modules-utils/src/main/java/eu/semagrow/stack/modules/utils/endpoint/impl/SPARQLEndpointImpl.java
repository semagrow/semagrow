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
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Value;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.QueryResult;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResultHandler;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.config.RepositoryConfigException;
import org.openrdf.repository.sparql.SPARQLConnection;
import org.openrdf.repository.sparql.SPARQLRepository;
import org.openrdf.repository.sparql.config.SPARQLRepositoryConfig;
import org.openrdf.repository.sparql.config.SPARQLRepositoryFactory;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.rdfxml.util.RDFXMLPrettyWriterFactory;

/**
 *
 * @author ggianna
 */
public class SPARQLEndpointImpl implements HttpHandler, SPARQLEndpoint {
    Map<UUID, HttpExchange> toServe;
    
    protected static String TEST_QUERY = 
            "PREFIX a: "
            + "<http://www.w3.org/2000/10/rdf-tests/j/file2.rdf>\n" +
                "SELECT ?name \n" +
                "WHERE {\n" +
                "?name rdf:type ?surname\n" +
                "}";
    private ReactivityParameters reactivityParameters;

    public SPARQLEndpointImpl() {
        try {
            // TODO: Read server settings from parameter file
            HttpServer server = HttpServer.create(new InetSocketAddress(8000), 
                    0);
            server.createContext("/endpoint", this);
            server.setExecutor(null); // creates a default executor
            server.start();
        } catch (IOException ex) {
            Logger.getLogger(SPARQLEndpointImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        // Init query map
        toServe = new HashMap<UUID, HttpExchange>();
    }

    
    public String getBaseURI() {
        // TODO: Change
        return "http://seamgrow.eu/";
    }
    
    
    public static void main(String[] saArgs) {
        SPARQLRepositoryConfig config = new SPARQLRepositoryConfig();
        config.setURL("http://www.semagrow.eu/");

        SPARQLRepositoryFactory factory = new SPARQLRepositoryFactory();
        SPARQLRepository repo;
        try {
            repo = factory.getRepository(config);
            RepositoryConnection rcConn;
            rcConn = (SPARQLConnection) repo.getConnection();
            Resource rG = new Resource() {

                public String stringValue() {
                    return "George";
                }
            };
            Value vName = new Value() {

                public String stringValue() {
                    return "foaf:name";
                }
            };
            rcConn.add(new StatementImpl(rG, 
                    new URIImpl("http://purl.org/dc/elements/1.1/#rdf:type"), 
                    vName));
            rcConn.commit();
            
            TupleQuery q = rcConn.prepareTupleQuery(QueryLanguage.SPARQL, TEST_QUERY);
            try {
                q.evaluate(new TupleQueryResultHandler() {
                    
                    public void startQueryResult(List<String> list) throws TupleQueryResultHandlerException {
                        System.err.println(list.toString());
                    }
                    
                    public void endQueryResult() throws TupleQueryResultHandlerException {
                        // IGNORE
                    }
                    
                    public void handleSolution(BindingSet bs) throws TupleQueryResultHandlerException {
                        
                        for (Binding bCur : bs) {
                            System.err.println(bCur.toString());
                        }
                    }
                });
            } catch (QueryEvaluationException ex) {
                Logger.getLogger(SPARQLEndpointImpl.class.getName()).log(Level.SEVERE, null, ex);
            } catch (TupleQueryResultHandlerException ex) {
                Logger.getLogger(SPARQLEndpointImpl.class.getName()).log(Level.SEVERE, null, ex);
            }
            
        } catch (RepositoryException ex) {
            Logger.getLogger(SPARQLEndpointImpl.class.getName()).log(Level.SEVERE, null, ex);
        } catch (RepositoryConfigException ex) {
            Logger.getLogger(SPARQLEndpointImpl.class.getName()).log(Level.SEVERE, null, ex);
        } catch (MalformedQueryException ex) {
            Logger.getLogger(SPARQLEndpointImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        
        
//        
//        
//        Repository hrRepos;
//        RepositoryConnection rcConn;
//        
//        String sLocalRepos = "http://localhost:8080/openrdf-workbench/repositories/10/";
//        // Initialize repository
//        try {
//            hrRepos = new SPARQLEndpointImpl(sLocalRepos);
//            hrRepos.initialize();
//        } catch (RepositoryException ex) {
//            Logger.getLogger(SPARQLEndpointImpl.class.getName()).log(
//                    Level.SEVERE, null, ex);
//            return;
//        }
//        
//        try {
//             rcConn = hrRepos.getConnection();
//        } catch (RepositoryException ex) {
//            Logger.getLogger(SPARQLEndpointImpl.class.getName()).log(
//                    Level.SEVERE, null, ex);
//            return;            
//        }
//        
//        try {
//            rcConn.prepareQuery(QueryLanguage.SPARQL, TEST_QUERY);
//        } catch (MalformedQueryException ex) {
//            Logger.getLogger(SPARQLEndpointImpl.class.getName()).log(
//                    Level.SEVERE, null, ex);
//            return;            
//        } catch (RepositoryException ex) {
//            Logger.getLogger(SPARQLEndpointImpl.class.getName()).log(
//                    Level.SEVERE, null, ex);
//            return;            
//        }
//        try {
//            // Finalize connection
//            hrRepos.shutDown();
//        } catch (RepositoryException ex) {
//            Logger.getLogger(SPARQLEndpointImpl.class.getName()).log(Level.SEVERE, null, ex);
//        }
    }

    /**
     * Returns result to the client that called.
     * @param res 
     */
    public void useQueryResult(QueryResult<BindingSet> res) {
        System.err.println("Result:" + res.toString());
    }

    public void handle(HttpExchange he) throws IOException {
        // Create unique ID
        UUID queryID = UUID.fromString(he.getRequestBody().toString());
        // Map exchange to id
        toServe.put(queryID, he);
        // Call decomposer
        QueryDecomposer qd = new QueryDecomposerImpl();
        
        // Get reactivity parameters
        // TODO: replace with constant parameter name
        String sStrategy = he.getAttribute("strategy").toString();
        int iTimeout = Integer.valueOf(he.getAttribute("strategy").toString());
        
        try {
            ReactivityParameters rpParams = new ReactivityParameters(iTimeout, 
                    sStrategy);
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
        RDFXMLPrettyWriterFactory factory = new RDFXMLPrettyWriterFactory();
        RDFWriter resultWriter = factory.getWriter(
                toServe.get(uQueryID).getResponseBody());
        
        
    }
}
