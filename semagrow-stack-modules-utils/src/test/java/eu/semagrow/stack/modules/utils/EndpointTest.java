/*
 *
 */

package eu.semagrow.stack.modules.utils;

import eu.semagrow.stack.modules.utils.endpoint.SPARQLEndpoint;
import eu.semagrow.stack.modules.utils.endpoint.impl.SPARQLEndpointImpl;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.httpclient.NameValuePair;
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
        try {
            se = new SPARQLEndpointImpl("http://127.0.0.1:18000/"
                    + "openrdf-sesame/repositories/SemagrowEndpoint");
        } catch (URISyntaxException ex) {
            Logger.getLogger(EndpointTest.class.getName()).log(Level.SEVERE, null, ex);
        }
        se.init();
        // TODO: Create hard coded repositories
        // from DBpedia (proxy)
        // from FactBook (proxy)
        // from SemanticBible (import data to local memory repos)
        // ...
        
    }

    final static String PREFIXES = "PREFIX dc:<http://purl.org/dc/elements/1.1/>\n" +
            "PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>\n" +
            "PREFIX owl:<http://www.w3.org/2002/07/owl#>\n" +
            "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
            "PREFIX rss:<http://purl.org/rss/1.0/>\n" +
            "PREFIX ntnames:<http://semanticbible.org/ns/2006/NTNames.owl#>\n" +
            "PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>\n" +
            "PREFIX ntind:<http://www.semanticbible.org/2005/09/NTN-individuals.owl#>\n" +
            "PREFIX ntonto:<http://semanticbible.org/ns/2006/NTNames#>\n" +
            "PREFIX dbo:<http://dbpedia.org/ontology/>\n" +
            "PREFIX dbpedia:<http://dbpedia.org/resource/>\n";
    
    
    final static String BIG_QUERY = "PREFIX  farm: <http://ontologies.seamless-ip.org/farm.owl#>\n" +
"PREFIX  dc:   <http://purl.org/dc/terms/>\n" +
"PREFIX  wgs84_pos: <http://www.w3.org/2003/01/geo/wgs84_pos#>\n" +
"PREFIX  t4f:  <http://semagrow.eu/schemas/t4f#>\n" +
"PREFIX  laflor: <http://semagrow.eu/schemas/laflor#>\n" +
"PREFIX  eururalis: <http://semagrow.eu/schemas/eururalis#>\n" +
"PREFIX  crop: <http://ontologies.seamless-ip.org/crop.owl#>\n" +
"\n" +
"SELECT  ?Longitude ?Latitude ?U\n" +
"WHERE\n" +
"  { { SELECT  ?Longitude ?Latitude (avg(?PR) AS ?PRE)\n" +
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
    
    
//    final static String BIG_QUERY = "PREFIX crop:      <http://ontologies.seamless-ip.org/crop.owl#>\n" +
//"PREFIX t4f:     <http://semagrow.eu/schemas/t4f#>\n" +
//"PREFIX farm:      <http://ontologies.seamless-ip.org/farm.owl#>\n" +
//"PREFIX laflor:      <http://semagrow.eu/schemas/laflor#>\n" +
//"PREFIX eururalis:      <http://semagrow.eu/schemas/eururalis#>\n" +
//"PREFIX wgs84_pos:      <http://www.w3.org/2003/01/geo/wgs84_pos#>\n" +
//"PREFIX sema: <http://semagrow.eu/schemas/semagrow#>\n" +
//            "SELECT ?Longitude ?Latitude (AVG(?PR) AS ?PRE) WHERE {\n" +
//"   ?R t4f:hasLong ?Longitude.\n" +
//"   ?R t4f:hasLat ?Latitude.\n" +
//"   ?R eururalis:landuse 11 .\n" +
//"   ?R t4f:precipitation ?PR.\n" +
//" }\n" +
//" GROUP BY ?Longitude ?Latitude";
    
    /**
     * Test method for 
     */
    @Test
    public void testLocalQuery() {
        // Check 10 DBPedia cities
        String sQuery = BIG_QUERY;
        // TODO: Re-enable
//                PREFIXES +
//            "\n" +
//            "SELECT ?city ?cityName WHERE\n" +
//            "{\n" +
//            " {?city rdf:type ntonto:City}\n" +
//            " {?city rdfs:label ?cityName}\n" +
//            "} LIMIT 10";
        try {        
            // DEBUG LINES
            System.out.println(sQuery);
            ////////
            querySPARQLEndpoint(sQuery);
            Logger.getLogger(EndpointTest.class.getName()).log(Level.INFO, 
                    "Waiting for the 10sec query to execute.");
            shutDownSPARQLEndpoint();
            Thread.sleep(10000);
        } catch (Exception ex) {
            Logger.getLogger(EndpointTest.class.getName()).log(Level.SEVERE, null, ex);
            assert(false);
        }
        assert(true);
    }

    // @Test
    public void testRemoteQuery() {
        // Check 10 DBPedia cities
        String sQuery = PREFIXES +
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
        String sQuery = PREFIXES +
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

    // @Test
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
            String sCallURL = String.format("%s",
                    se.getBaseURI());
//            , // Endpoint URI
//                    SPARQLEndpointImpl.STRATEGY_PARAM_NAME,  // Strategy param
//                    ReactivityParameters.STRATEGY_DELIVER_ON_COMPLETION, // Selected strategy
//                    SPARQLEndpointImpl.QUERY_PARAM_NAME, // Query param
//                    URLEncoder.encode(sQuery, "utf8"), // Query
//                    SPARQLEndpointImpl.TIMEOUT_PARAM_NAME, // Timeout param
//                    5);// Timeout
            
            // TODO: REMOVE
            //HTTPRepository h = new HTTPRepository("http://localhost:8080/openrdf-sesame/repositories/FedStore ");
//            ParsedQuery pq = QueryParserUtil.parseQuery(QueryLanguage.SPARQL, 
//                    BIG_QUERY, "http://localhost/");
//            System.err.println(pq.getTupleExpr().toString());
            ///////////////
            
            
            URL myURL = new URL(sCallURL);
//            URLConnection myURLConnection = myURL.openConnection();
            
            // Use POST connection
            HttpURLConnection conn = new 
              sun.net.www.protocol.http.HttpURLConnection(myURL, Proxy.NO_PROXY);
            conn.setReadTimeout(1000000);
            conn.setConnectTimeout(1500000);
            conn.setRequestMethod("POST");
            conn.setDoInput(true);
            conn.setDoOutput(true);

            List<NameValuePair> params = new ArrayList<NameValuePair>();
            params.add(new NameValuePair(SPARQLEndpointImpl.STRATEGY_PARAM_NAME, 
                    ReactivityParameters.STRATEGY_DELIVER_ON_COMPLETION));
            params.add(new NameValuePair(SPARQLEndpointImpl.QUERY_PARAM_NAME, sQuery));
            params.add(new NameValuePair(SPARQLEndpointImpl.TIMEOUT_PARAM_NAME, "5"));

            OutputStream os = conn.getOutputStream();
            BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(os, "UTF-8"));
            writer.write(getQuery(params));
            writer.flush();
            writer.close();
            os.close();            
            conn.connect();
            
            InputStream is = conn.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            StringBuffer sRes = new StringBuffer();
            // Get result to output
            String sLine = br.readLine();
            while (sLine != null) {
                sRes.append(sLine);
                sLine = br.readLine();
            }
            // Finalize
            is.close();
            // Output result
            Logger.getLogger(EndpointTest.class.getName()).info(sRes.toString());
            
        } 
        catch (MalformedURLException e) { 
            Logger.getLogger(EndpointTest.class.getName()).log(Level.SEVERE, null, e);
            assert(false);
        } 
        catch (IOException e) {   
            // openConnection() failed
            Logger.getLogger(EndpointTest.class.getName()).log(Level.SEVERE, null, e);
            assert(false);
        }

    }

    private String getQuery(List<NameValuePair> params) throws UnsupportedEncodingException
    {
        StringBuilder result = new StringBuilder();
        boolean first = true;

        for (NameValuePair pair : params)
        {
            if (first)
                first = false;
            else
                result.append("&");

            result.append(URLEncoder.encode(pair.getName(), "UTF-8"));
            result.append("=");
            result.append(URLEncoder.encode(pair.getValue(), "UTF-8"));
        }

        return result.toString();
    }

    private void shutDownSPARQLEndpoint() {
        try {
            String sCallURL = se.getBaseURI() + SPARQLEndpointImpl.SERVER_STOP_SUFFIX;
            URL myURL = new URL(sCallURL);
            URLConnection myURLConnection = myURL.openConnection();
            myURLConnection.connect();
        } catch (IOException ex) {
            Logger.getLogger(EndpointTest.class.getName()).log(Level.SEVERE, null, ex);
        }
        Logger.getLogger(EndpointTest.class.getName()).log(Level.INFO, 
                "Shutting down server...");
    }
}
    