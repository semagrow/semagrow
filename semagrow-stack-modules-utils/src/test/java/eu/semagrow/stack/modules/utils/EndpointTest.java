/*
 *
 */

package eu.semagrow.stack.modules.utils;

import eu.semagrow.stack.modules.utils.endpoint.SPARQLEndpoint;
import eu.semagrow.stack.modules.utils.endpoint.impl.SPARQLEndpointImpl;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author ggianna
 */
public class EndpointTest {
/**
 * Using endpoints DBPedia, FactBook and http://www.semanticbible.com/ntn/ntn-view.html 
 */
    SPARQLEndpoint se;

    @Before
    public void setUp() {
        se = new SPARQLEndpointImpl();
        se.init();
        // TODO: Create hard coded repositories
        // from DBpedia (proxy)
        // from FactBook (proxy)
        // from SemanticBible (import data to local memory repos)
        // ...
        
    }

    /**
     * Test method for 
     */
    @Test
    public void testLocalQuery() {
        // Check 10 DBPedia cities
        String sQuery = "PREFIX dc:<http://purl.org/dc/elements/1.1/>\n" +
            "PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>\n" +
            "PREFIX owl:<http://www.w3.org/2002/07/owl#>\n" +
            "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
            "PREFIX rss:<http://purl.org/rss/1.0/>\n" +
            "PREFIX ntnames:<http://semanticbible.org/ns/2006/NTNames.owl#>\n" +
            "PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>\n" +
            "PREFIX ntind:<http://www.semanticbible.org/2005/09/NTN-individuals.owl#>\n" +
            "PREFIX ntonto:<http://semanticbible.org/ns/2006/NTNames#>\n" +
            "PREFIX dbo:<http://dbpedia.org/ontology/>\n" +
            "PREFIX dbpedia:<http://dbpedia.org/resource/>\n" +
            "\n" +
            "SELECT ?city ?cityName WHERE\n" +
            "{\n" +
            " {?city rdf:type ntonto:City}\n" +
            " {?city rdfs:label ?cityName}\n" +
            "}";
        try {        
            querySPARQLEndpoint(sQuery);
        } catch (Exception ex) {
            Logger.getLogger(EndpointTest.class.getName()).log(Level.SEVERE, null, ex);
            assert(false);
        }
        assert(true);
    }

    // @Test
    public void testRemoteQuery() {
        // Check 10 DBPedia cities
        String sQuery = "PREFIX dc:<http://purl.org/dc/elements/1.1/>\n" +
            "PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>\n" +
            "PREFIX owl:<http://www.w3.org/2002/07/owl#>\n" +
            "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
            "PREFIX rss:<http://purl.org/rss/1.0/>\n" +
            "PREFIX ntnames:<http://semanticbible.org/ns/2006/NTNames.owl#>\n" +
            "PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>\n" +
            "PREFIX ntind:<http://www.semanticbible.org/2005/09/NTN-individuals.owl#>\n" +
            "PREFIX ntonto:<http://semanticbible.org/ns/2006/NTNames#>\n" +
            "PREFIX dbo:<http://dbpedia.org/ontology/>\n" +
            "PREFIX dbpedia:<http://dbpedia.org/resource/>\n" +
            "\n" +
            "SELECT ?y ?z WHERE\n" +
            "{\n" +
            " SERVICE <http://dbpedia.org/sparql> {?y rdf:type dbo:City}\n" +
            " {?y rdfs:label ?z}\n" +
            "}";
        assert(true);
    }

    /**
     * Test method for 
     */
    // @Test
    public void testDistributedQuery() {
        // Combine new testament cities
        // with DBPedia cities, based on label
        String sQuery = "PREFIX dc:<http://purl.org/dc/elements/1.1/>\n" +
            "PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>\n" +
            "PREFIX owl:<http://www.w3.org/2002/07/owl#>\n" +
            "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
            "PREFIX rss:<http://purl.org/rss/1.0/>\n" +
            "PREFIX ntnames:<http://semanticbible.org/ns/2006/NTNames.owl#>\n" +
            "PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>\n" +
            "PREFIX ntind:<http://www.semanticbible.org/2005/09/NTN-individuals.owl#>\n" +
            "PREFIX ntonto:<http://semanticbible.org/ns/2006/NTNames#>\n" +
            "PREFIX dbo:<http://dbpedia.org/ontology/>\n" +
            "PREFIX dbpedia:<http://dbpedia.org/resource/>\n" +
            "\n" +
            "SELECT ?x WHERE\n" +
            "{\n" +
            " {?x rdf:type ntonto:City}.\n" +
            " SERVICE <http://dbpedia.org/sparql> {?y rdf:type dbo:City}\n" +
            " {?x rdfs:label ?z}\n" +
            " {?y rdfs:label ?z}\n" +
            "}";
        
        assert(true);
    }

    @Test
    public void testDistributedQueryWithFallback() {
        assert(true);
    }

    @After
    public void cleanUp(){
        // Terminate server
        se.cleanUp();
    }
    
    protected void querySPARQLEndpoint(String sQuery) throws Exception {
        try {
            // TODO: Use parameter for 5 seconds of maximum query time
            String sCallURL = String.format("%s?%s=%s&%s=%s&%s=%d",
                    se.getBaseURI(), // Endpoint URI
                    SPARQLEndpointImpl.STRATEGY_PARAM_NAME,  // Strategy param
                    ReactivityParameters.STRATEGY_DELIVER_ON_COMPLETION, // Selected strategy
                    SPARQLEndpointImpl.QUERY_PARAM_NAME, // Query param
                    URLEncoder.encode(sQuery, "utf8"), // Query
                    SPARQLEndpointImpl.TIMEOUT_PARAM_NAME, // Timeout param
                    5);// Timeout
            URL myURL = new URL(sCallURL);
            URLConnection myURLConnection = myURL.openConnection();
            myURLConnection.connect();
            StringWriter os = new StringWriter();
            InputStream is = myURLConnection.getInputStream();
            // Get result to output
            int iNext = is.read();
            while (iNext > -1) { 
                os.write(iNext);
                iNext = is.read();
            }
            // Finalize
            is.close();
            os.close();
            // Output result
            System.out.println(os.toString());
            
        } 
        catch (MalformedURLException e) { 
            Logger.getLogger(EndpointTest.class.getName()).log(Level.SEVERE, null, e);
            assert(false);
            
            // new URL() failed
            // ...
            assert(false);
        } 
        catch (IOException e) {   
            // openConnection() failed
            Logger.getLogger(EndpointTest.class.getName()).log(Level.SEVERE, null, e);
            assert(false);
            
            // ...
        }

    }
}
    