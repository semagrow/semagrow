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

import eu.semagrow.stack.modules.utils.QueryTranformation;
import eu.semagrow.stack.modules.utils.ReactivityParameters;
import eu.semagrow.stack.modules.utils.ResourceSelector;
import eu.semagrow.stack.modules.utils.SelectedResource;
import eu.semagrow.stack.modules.utils.endpoint.SPARQLEndpoint;
import eu.semagrow.stack.modules.utils.endpoint.impl.SPARQLEndpointImpl;
import eu.semagrow.stack.modules.utils.federationWrapper.FederationEndpointWrapperComponent;
import eu.semagrow.stack.modules.utils.federationWrapper.impl.FederationEndpointWrapperComponentImpl;
import eu.semagrow.stack.modules.utils.impl.QueryTransformationImpl;
import eu.semagrow.stack.modules.utils.impl.ResourceSelectorImpl;
import eu.semagrow.stack.modules.utils.queryDecomposition.DataSourceSelector;
import eu.semagrow.stack.modules.utils.queryDecomposition.DistributedExecutionPlan;
import eu.semagrow.stack.modules.utils.queryDecomposition.QueryDecomposer;
import eu.semagrow.stack.modules.utils.queryDecomposition.QueryDecompositionComponent;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.OpenRDFException;
import org.openrdf.query.Query;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.ParsedTupleQuery;
import org.openrdf.query.parser.QueryParserUtil;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.http.HTTPRepository;
import org.openrdf.repository.sail.SailQuery;



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
    Map<UUID, Iterator<DistributedExecutionPlan>> plansPerQuery;
    
    // TODO: Replace with parameter
    protected final String FEDERATION_REPOS_URL = "http://localhost:8080/"
            + "openrdf-workbench/repositories/10/";
    
    public QueryDecompositionComponentImpl(SPARQLEndpoint seiCaller) {
        // Init decomposer
        decomposer = new QueryDecomposerImpl();            
        // Init resource selector
        resourceSelector = new ResourceSelectorImpl();
        // Set caller
        callerEndpoint = seiCaller;
        // Init plans per query
        plansPerQuery = new HashMap<UUID, Iterator<DistributedExecutionPlan>>();
    }

    
    /**
     * Decomposes a SPARQL query, also forwarding it for distributed execution on
     * the federation.
     * @param caller The calling endpoint.
     * @param uQueryID The unique ID of the query.
     * @param sQuery The text of the SPARQL query.
     * @param rpParams The reactivity parameters for the query.
     */
    public void decompose(final SPARQLEndpoint caller, final UUID uQueryID, 
            final String sQuery, final ReactivityParameters rpParams) {
        // Decompose query
        try {
            // Get statement patterns
            List<StatementPattern> lspPatterns = decomposer.decomposeQuery(caller, 
                    uQueryID, sQuery);
            
            // Init data source selector
            dataSourceSelector = new DataSourceSelectorImpl(rpParams);
            
            // For every pattern
            for (StatementPattern spCurNode : lspPatterns) {
                // Get sources
                // TODO: check measurement_id (3???)
                List<SelectedResource> lsrCurResources = 
                        resourceSelector.getSelectedResources(spCurNode, 3);
                
                // Add selected as alternatives for planning
                dataSourceSelector.addFragmentInfo(spCurNode, lsrCurResources);                
            }
            // Update plans per query map
            plansPerQuery.put(uQueryID, dataSourceSelector.getPlans(
                    sQuery).iterator());
            
            
        } catch (OpenRDFException ex) {
            Logger.getLogger(QueryDecompositionComponentImpl.class.getName()
                ).log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
            Logger.getLogger(QueryDecompositionComponentImpl.class.getName()
                ).log(Level.SEVERE, "Possibly not a valid query...", ex);
        }
        
        // TODO: Perform transformation
        QueryTranformation qtTransformer = new QueryTransformationImpl(null);
                
        
        // Call federation wrapper
        final FederationEndpointWrapperComponent fedWrapper = new 
            FederationEndpointWrapperComponentImpl(qtTransformer, 
                    new HTTPRepository(FEDERATION_REPOS_URL));
        
        new Thread(new Runnable() {

            public void run() {
                // Execute given query asynchronously
                fedWrapper.executeDistributedQuery(callerEndpoint, sQuery, 
                        uQueryID, plansForQuery(uQueryID), rpParams);
            }
        });
    }

    public Iterator<DistributedExecutionPlan> plansForQuery(UUID queryID) {
        return plansPerQuery.get(queryID);
    }
}

