/*
 * 
 */

package eu.semagrow.stack.modules.utils.queryDecomposition.impl;

import eu.semagrow.stack.modules.utils.endpoint.SPARQLEndpoint;
import eu.semagrow.stack.modules.utils.queryDecomposition.QueryDecomposer;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.UnsupportedQueryLanguageException;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.ParsedQuery;

/**
 *
 * @author ggianna
 */
public class QueryDecomposerImpl implements QueryDecomposer {
    public List<StatementPattern> getPatterns(SPARQLEndpoint caller, 
            UUID uQueryID, ParsedQuery pq)
    {
        List<StatementPattern> lspPatterns = null;
        // Get model
        QueryModelNode qmnRoot = pq.getTupleExpr();

        // Collect all patterns
        StatementPatternCollector spc = new StatementPatternCollector();
        qmnRoot.visit(spc);
        lspPatterns = spc.getStatementPatterns();
        
        return lspPatterns;
    }
    
}
