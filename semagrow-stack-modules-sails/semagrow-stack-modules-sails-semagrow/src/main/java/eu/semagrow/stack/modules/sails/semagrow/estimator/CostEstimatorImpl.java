package eu.semagrow.stack.modules.sails.semagrow.estimator;

import eu.semagrow.stack.modules.api.ResourceSelector;
import eu.semagrow.stack.modules.querydecomp.estimator.CardinalityEstimator;
import eu.semagrow.stack.modules.querydecomp.estimator.CostEstimator;
import eu.semagrow.stack.modules.sails.semagrow.algebra.BindJoin;
import eu.semagrow.stack.modules.sails.semagrow.algebra.HashJoin;
import eu.semagrow.stack.modules.sails.semagrow.algebra.SourceQuery;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Union;

/**
 * Created by angel on 4/28/14.
 */
public class CostEstimatorImpl implements CostEstimator {

    private CardinalityEstimator cardinalityEstimator;


    public CostEstimatorImpl(CardinalityEstimator cardinalityEstimator) {
        this.cardinalityEstimator = cardinalityEstimator;
    }

    /**
     * @param expr
     * @return
     */
    public long getCost(TupleExpr expr) {

        // just favor remote queries.
        if (expr instanceof SourceQuery)
            return getCost((SourceQuery)expr);
        else
            return 1;
    }

    public long getCost(SourceQuery expr) {

        // cardinality on a specific endpoint.
        // communication cost of the endpoint * cardinality
        // processing cost + load (it may depends on the complexity of the query)

        //int count = cardinalityEstimator.getCardinality(expr);

        //totalCost = processingCost(subexpr) + communicationCost(count, source) + initializationCostOfQuery;

        return 0;
    }

    public long getCost(BindJoin join) {
        // long cardinalityOfLeft = cardinalityEstimator.getCardinality(join.getLeftArg());
        // long costLeftArgument = estimateCost(join.getLeftArg());
        // long cardinalityAll = cardinalityEstimator.getCardinality(join);
        // long queries = cardinalityOfLeft / bindingsPerQuery;
        // long resultsPerQuery = cardinalityAll / queries;
        // totalCost = costLeftArgument + queries * costOfRightArgumentWithBinding
        return 0;
    }

    public long getCost(HashJoin join) {
        return 0;
    }

    public long getCost(Union union) {
        return 0;
    }


}
