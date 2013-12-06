/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package eu.semagrow.stack.modules.utils.queryDecomposition.impl;

import eu.semagrow.stack.modules.utils.endpoint.SPARQLEndpoint;
import eu.semagrow.stack.modules.utils.queryDecomposition.QueryDecomposer;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.UnsupportedQueryLanguageException;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.ParsedTupleQuery;
import org.openrdf.query.parser.QueryParserUtil;

/**
 *
 * @author ggianna
 */
public class QueryDecomposerImpl implements QueryDecomposer {

    public List<StatementPattern> decomposeQuery(SPARQLEndpoint caller, 
            UUID uQueryID, String sQuery) throws MalformedQueryException 
    {
        List<StatementPattern> lspPatterns = null;
        try {
            // Parse query
            ParsedTupleQuery pq = (ParsedTupleQuery)QueryParserUtil.parseQuery(
                    QueryLanguage.SPARQL, sQuery, 
                    caller.getBaseURI());
            
            // Get model
            QueryModelNode qmnRoot = pq.getTupleExpr();
            
            // Collect all patterns
            StatementPatternCollector spc = new StatementPatternCollector();
            qmnRoot.visit(spc);
            lspPatterns = spc.getStatementPatterns();
        } catch (UnsupportedQueryLanguageException ex) {
            Logger.getLogger(QueryDecomposerImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return lspPatterns;
    }
    
}
