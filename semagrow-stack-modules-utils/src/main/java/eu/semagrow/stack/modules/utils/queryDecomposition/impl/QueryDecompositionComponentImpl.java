/* 
 * Licensed to Aduna under one or more contributor license agreements.  
 * See the NOTICE.txt file distributed with this work for additional 
 * information regarding copyright ownership. 
 *
 * Aduna licenses this file to you under the terms of the Aduna BSD 
 * License (the "License"); you may not use this file except in compliance 
 * with the License. See the LICENSE.txt file distributed with this work 
 * for the full License.
 *
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or 
 * implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
/*
 * 
 */

package eu.semagrow.stack.modules.utils.queryDecomposition.impl;

import eu.semagrow.stack.modules.utils.ReactivityParameters;
import eu.semagrow.stack.modules.utils.endpoint.SPARQLEndpoint;
import eu.semagrow.stack.modules.utils.endpoint.impl.SPARQLEndpointImpl;
import eu.semagrow.stack.modules.utils.queryDecomposition.AlternativeDecomposition;
import eu.semagrow.stack.modules.utils.queryDecomposition.DataSourceSelector;
import eu.semagrow.stack.modules.utils.queryDecomposition.QueryDecomposer;
import eu.semagrow.stack.modules.utils.queryDecomposition.QueryDecompositionComponent;
import eu.semagrow.stack.modules.utils.resourceselector.ResourceSelector;
import eu.semagrow.stack.modules.utils.resourceselector.SelectedResource;
import eu.semagrow.stack.modules.utils.resourceselector.impl.ResourceSelectorImpl;
import java.net.URI;
import java.util.ArrayList;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.UnsupportedQueryLanguageException;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.QueryParserUtil;



/**
 *
 * @author ggianna
 */
public class QueryDecompositionComponentImpl implements 
        QueryDecompositionComponent {

    ResourceSelector resourceSelector;
    QueryDecomposer decomposer;
    SPARQLEndpoint callerEndpoint;
    DataSourceSelector dataSourceSelector;
    Map<UUID, Iterator<AlternativeDecomposition>> plansPerQuery;
    
    // TODO: Replace with parameter
    // protected final String FEDERATION_REPOS_URL = "http://localhost:8080/"
    //        + "openrdf-workbench/repositories/10/";
    
    public QueryDecompositionComponentImpl(SPARQLEndpoint seiCaller) {
        // Init decomposer
        decomposer = new QueryDecomposerImpl();            
        // Init resource selector
        resourceSelector = new ResourceSelectorImpl();
        // Set caller
        callerEndpoint = seiCaller;
        // Init plans per query
        plansPerQuery = new HashMap<UUID, Iterator<AlternativeDecomposition>>();
    }

    
    /**
     * Decomposes a SPARQL query, creating a set of alternative decompositions
     * usable by the federation wrapper component.
     * @param caller The calling endpoint.
     * @param uQueryID The unique ID of the query.
     * @param sQuery The text of the SPARQL query.
     * @param rpParams The reactivity parameters for the query.
     */
    public Iterator<AlternativeDecomposition> decompose(
            final SPARQLEndpoint caller, final UUID uQueryID, 
            final ParsedQuery pqQuery, final ReactivityParameters rpParams) {
        // Decompose query
        try {
            // Get statement patterns
            List<StatementPattern> lspPatterns = decomposer.getPatterns(caller, 
                    uQueryID, pqQuery);
            
            // Init data source selector
            dataSourceSelector = new DataSourceSelectorImpl(rpParams);
            
            // For every pattern
            for (StatementPattern spCurNode : lspPatterns) {
                // Get sources
                // TODO: check measurement_id (0???)
                List<SelectedResource> lsrCurResources = 
                        resourceSelector.getSelectedResources(spCurNode, 1);
                
                // Add selected as alternatives for planning
                dataSourceSelector.addStatementInfo(spCurNode, lsrCurResources);
            }
            // TODO: Implement normally
            // DEMO START
            AlternativeDecomposition ad = getDemoDecomposition();
            List<AlternativeDecomposition> lDecomps = new ArrayList();
            lDecomps.add(ad);
            // Update possible decomposition list for query
            plansPerQuery.put(uQueryID, lDecomps.iterator());
            
            // DEMO END
        } catch (Exception ex) {
            Logger.getLogger(QueryDecompositionComponentImpl.class.getName()
                ).log(Level.SEVERE, "Possibly not a valid query...", ex);
        }
        
        // Return caclulated plans
        return plansPerQuery.get(uQueryID);
    }

    public Iterator<AlternativeDecomposition> decompositionsForQuery(UUID queryID) {
        return plansPerQuery.get(queryID);
    }

    private final String DEMO_PREFIX = "PREFIX  farm: <http://ontologies.seamless-ip.org/farm.owl#>\n" +
"PREFIX  dc:   <http://purl.org/dc/terms/>\n" +
"PREFIX  wgs84_pos: <http://www.w3.org/2003/01/geo/wgs84_pos#>\n" +
"PREFIX  t4f:  <http://semagrow.eu/schemas/t4f#>\n" +
"PREFIX  laflor: <http://semagrow.eu/schemas/laflor#>\n" +
"PREFIX  eururalis: <http://semagrow.eu/schemas/eururalis#>\n" +
"PREFIX  crop: <http://ontologies.seamless-ip.org/crop.owl#>";
    
    
    private final String FIRST_FRAG = DEMO_PREFIX +
        "\n" +
        "SELECT  ?Longitude ?Latitude (avg(?PR) AS ?PRE)\n" +
        "WHERE\n" +
        "  { ?R t4f:hasLong ?Longitude .\n" +
        "    ?R t4f:hasLat ?Latitude .\n" +
        "    ?R eururalis:landuse \"11\" .\n" +
        "    ?R t4f:precipitation ?PR\n" +
        "  }\n" +
"GROUP BY ?Longitude ?Latitude";
    
    private final String SECOND_FRAG = DEMO_PREFIX + 
        "\n" +
        "SELECT  ?F ?C ?A ?D ?PRE2\n" +
        "WHERE\n" +
        "  { ?F farm:year 2010 .\n" +
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
        "  }";

    private final String THIRD_FRAG = DEMO_PREFIX + "\n" +
            "SELECT  ?J ?U ?P ?ALAT2 ?ALONG2\n" +
            "WHERE\n" +
            "  { ?J dc:subject <http://aims.fao.org/aos/agrovoc/xl_en_1299487055215> .\n" +
            "    ?J laflor:location ?U .\n" +
            "    ?J laflor:language <http://id.loc.gov/vocabulary/iso639-2/es> .\n" +
            "    ?J <http://schema.org/about> ?P .\n" +
            "    ?P wgs84_pos:lat ?ALAT2 .\n" +
            "    ?P wgs84_pos:long ?ALONG2\n" +
            "  }";
    
    private AlternativeDecomposition getDemoDecomposition() {
        AlternativeDecomposition ad = new AlternativeDecompositionImpl();
        
        // Queries
        ParsedQuery p1 = null;
        ParsedQuery p2 = null;
        ParsedQuery p3 = null;
        try {
            p1 = QueryParserUtil.parseQuery(QueryLanguage.SPARQL, 
                    FIRST_FRAG, "http://semagrow.eu/");
            p2 = QueryParserUtil.parseQuery(QueryLanguage.SPARQL, 
                    SECOND_FRAG, "http://semagrow.eu/");
            p3 = QueryParserUtil.parseQuery(QueryLanguage.SPARQL, 
                    THIRD_FRAG, "http://semagrow.eu/");
        } catch (MalformedQueryException ex) {
            Logger.getLogger(SPARQLEndpointImpl.class.getName()).log(Level.WARNING
                    , "Malformed query",  ex);
            return ad;
        } catch (UnsupportedQueryLanguageException ex) {
            Logger.getLogger(SPARQLEndpointImpl.class.getName()).log(Level.WARNING
                    , "Unsupported query language. Should NOT happen if SPARQL is used."
                            + "Please check program.",  ex);
            return ad;
        }
        
        // Endpoints
        ad.add(new RemoteQueryFragmentImpl(p1, 
                URIListFromItem("http://www.semagrow.eu:8080/bigdata_t4f/sparql")));
        ad.add(new RemoteQueryFragmentImpl(p2, 
                URIListFromItem("http://www.semagrow.eu:8080/bigdata_seamless/sparql")));
        ad.add(new RemoteQueryFragmentImpl(p3, 
                URIListFromItem("http://www.semagrow.eu:8080/bigdata_laflor/sparql")));
        
        // Return dummy decomposition
        return ad;
    }
    
    private List<URI> URIListFromItem(String sURL) {
        ArrayList<URI> alRes = new ArrayList();
        alRes.add(URI.create(sURL));
        return alRes;
    }
}

