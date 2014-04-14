/*
 *
 */

package eu.semagrow.stack.modules.utils.queryDecomposition.impl;

import eu.semagrow.stack.modules.utils.queryDecomposition.AlternativeDecomposition;
import eu.semagrow.stack.modules.utils.queryDecomposition.DecompositionStrategySelector;
import eu.semagrow.stack.modules.utils.queryDecomposition.RemoteQueryFragment;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.ParsedTupleQuery;

/**
 *
 * @author ggianna
 */
public class DecompositionStrategySelectorImpl implements DecompositionStrategySelector,
        Comparator {

    public Collection<AlternativeDecomposition> rankDecompositions(
            Collection<AlternativeDecomposition> possibleDecompositions) {
        ArrayList alRes = new ArrayList();
        alRes.addAll(possibleDecompositions);
        
        Collections.sort(alRes, this);
        return alRes;
    }

    public int compare(Object o1, Object o2) {
        AlternativeDecomposition ad1 = (AlternativeDecomposition)o1;
        AlternativeDecomposition ad2 = (AlternativeDecomposition)o2;
        // Return the difference of the evaluation
        int iRes = evaluate(ad1) - evaluate(ad2);
        // TODO: Handle equivalences
        return iRes;
    }
    
    protected int evaluate(AlternativeDecomposition ad) {
        // Init max
        int iMaxFragmentVarCnt = 0;
        // For every fragment
        for (RemoteQueryFragment rq : ad) {
            // TODO: Handle other types of queries
            ParsedTupleQuery pq = (ParsedTupleQuery)rq.getFragment();
            QueryModelNode qmnRoot = pq.getTupleExpr();

            int iCurFragVarCnt = 0;
            // Collect all patterns
            StatementPatternCollector spc = new StatementPatternCollector();        
            List<StatementPattern> lsPatterns = spc.getStatementPatterns();
            // For every pattern get all variables
            for (StatementPattern spCur : lsPatterns) {
                iCurFragVarCnt += spCur.getVarList().size();
            }
            
            // Update maximum number of variables per fragment if required
            if (iCurFragVarCnt > iMaxFragmentVarCnt)
                iMaxFragmentVarCnt = iCurFragVarCnt;
        }
        // Return the maximum as evaluation score
        return iMaxFragmentVarCnt;
    }
}
