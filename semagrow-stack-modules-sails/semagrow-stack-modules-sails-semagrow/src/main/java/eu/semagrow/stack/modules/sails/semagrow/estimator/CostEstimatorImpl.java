package eu.semagrow.stack.modules.sails.semagrow.estimator;

import eu.semagrow.stack.modules.api.ResourceSelector;
import eu.semagrow.stack.modules.querydecomp.estimator.CardinalityEstimator;
import eu.semagrow.stack.modules.querydecomp.estimator.CostEstimator;
import eu.semagrow.stack.modules.sails.semagrow.algebra.BindJoin;
import eu.semagrow.stack.modules.sails.semagrow.algebra.HashJoin;
import eu.semagrow.stack.modules.sails.semagrow.algebra.SourceQuery;
import org.openrdf.model.URI;
import org.openrdf.query.algebra.*;
import sun.org.mozilla.javascript.ast.UnaryExpression;

import javax.naming.BinaryRefAddr;

/**
 * Created by angel on 4/28/14.
 */
public class CostEstimatorImpl implements CostEstimator {

    private CardinalityEstimator cardinalityEstimator;

    private static int C_TRANSFER_TUPLE = 1;
    private static int C_TRANSFER_QUERY = 5;

    public CostEstimatorImpl(CardinalityEstimator cardinalityEstimator) {
        this.cardinalityEstimator = cardinalityEstimator;
    }

    /**
     * @param expr
     * @return
     */
    public double getCost(TupleExpr expr) {

        // just favor remote queries.
        if (expr instanceof SourceQuery)
            return getCost((SourceQuery)expr);
        else if (expr instanceof Join)
            return getCost((Join)expr);
        else
            return 1;
    }

    public double getCost(SourceQuery expr) {

        // cardinality on a specific endpoint.
        // communication cost of the endpoint * cardinality
        // processing cost + load (it may depends on the complexity of the query)

        //int count = cardinalityEstimator.getCardinality(expr);

        //totalCost = processingCost(subexpr) + communicationCost(count, source) + initializationCostOfQuery;

        double cost = 0;
        for (URI src : expr.getSources()) {
            cost += cardinalityEstimator.getCardinality(expr) * C_TRANSFER_TUPLE + C_TRANSFER_QUERY;
        }
        return cost;
    }

    public double getCost(BindJoin join) {
        // long cardinalityOfLeft = cardinalityEstimator.getCardinality(join.getLeftArg());
        // long costLeftArgument = estimateCost(join.getLeftArg());
        // long cardinalityAll = cardinalityEstimator.getCardinality(join);
        // long queries = cardinalityOfLeft / bindingsPerQuery;
        // long resultsPerQuery = cardinalityAll / queries;
        // totalCost = costLeftArgument + queries * costOfRightArgumentWithBinding

        long leftCard = cardinalityEstimator.getCardinality(join.getLeftArg());
        long joinCard = cardinalityEstimator.getCardinality(join);

        return leftCard * (C_TRANSFER_QUERY + C_TRANSFER_TUPLE) + joinCard * C_TRANSFER_TUPLE;
    }

    public double getCost(HashJoin join) {
        long leftCard = cardinalityEstimator.getCardinality(join.getLeftArg());
        long rightCard = cardinalityEstimator.getCardinality(join.getRightArg());

        return (leftCard + rightCard) * C_TRANSFER_TUPLE + 2 * C_TRANSFER_QUERY;
    }

    public double getCost(Join join) {
        long leftCard = cardinalityEstimator.getCardinality(join.getLeftArg());
        long rightCard = cardinalityEstimator.getCardinality(join.getRightArg());

        return (leftCard + rightCard) * C_TRANSFER_TUPLE + 2 * C_TRANSFER_QUERY;
    }

    public double getCost(UnaryTupleOperator expr) {
        return getCost(expr.getArg());
    }

    public double getCost(BinaryTupleOperator expr) {
        return getCost(expr.getLeftArg()) + getCost(expr.getRightArg());
    }
}
