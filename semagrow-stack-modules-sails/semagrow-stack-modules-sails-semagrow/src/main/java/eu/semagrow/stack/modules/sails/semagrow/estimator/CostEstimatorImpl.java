package eu.semagrow.stack.modules.sails.semagrow.estimator;

import eu.semagrow.stack.modules.api.estimator.CardinalityEstimator;
import eu.semagrow.stack.modules.api.estimator.CostEstimator;
import eu.semagrow.stack.modules.sails.semagrow.algebra.BindJoin;
import eu.semagrow.stack.modules.sails.semagrow.algebra.HashJoin;
import eu.semagrow.stack.modules.sails.semagrow.algebra.MergeJoin;
import eu.semagrow.stack.modules.sails.semagrow.algebra.SourceQuery;
import eu.semagrow.stack.modules.sails.semagrow.optimizer.Plan;
import org.openrdf.model.URI;
import org.openrdf.query.algebra.*;

/**
 * Created by angel on 4/28/14.
 */
public class CostEstimatorImpl implements CostEstimator {

    private CardinalityEstimator cardinalityEstimator;

    private static double C_TRANSFER_TUPLE = 0.001;
    private static double C_TRANSFER_QUERY = 0.005;

    private static double C_PROBE_TUPLE = 0.001;   //cost to probe a tuple against a hash table
    private static double C_HASH_TUPLE = 0.003;    //cost to hash a tuple to a hash table

    public CostEstimatorImpl(CardinalityEstimator cardinalityEstimator) {
        this.cardinalityEstimator = cardinalityEstimator;
    }

    /**
     * @param expr
     * @return
     */
    public double getCost(TupleExpr expr) {
        return getCost(expr, null);
    }

    public double getCost(TupleExpr expr, URI source) {
        // just favor remote queries.
        if (expr instanceof SourceQuery)
            return getCost((SourceQuery)expr, source);
        else if (expr instanceof Join)
            return getCost((Join)expr, source);
        else if (expr instanceof Order)
            return getCost((Order)expr, source);
        else if (expr instanceof Plan)
            return ((Plan)expr).getCost();
        else
            return cardinalityEstimator.getCardinality(expr, source)/1000;
    }

    public double getCost(SourceQuery expr, URI source) {

        // cardinality on a specific endpoint.
        // communication cost of the endpoint * cardinality
        // processing cost + load (it may depends on the complexity of the query)

        //int count = cardinalityEstimator.getCardinality(expr);

        //totalCost = processingCost(subexpr) + communicationCost(count, source) + initializationCostOfQuery;

        double communCost = C_TRANSFER_QUERY +
                cardinalityEstimator.getCardinality(expr.getArg()) * C_TRANSFER_TUPLE;

        double cost = getCost(expr.getArg()) + communCost;

        return cost;
    }

    public double getCost(BindJoin join, URI source) {
        // long cardinalityOfLeft = cardinalityEstimator.getCardinality(join.getLeftArg());
        // long costLeftArgument = estimateCost(join.getLeftArg());
        // long cardinalityAll = cardinalityEstimator.getCardinality(join);
        // long queries = cardinalityOfLeft / bindingsPerQuery;
        // long resultsPerQuery = cardinalityAll / queries;
        // totalCost = costLeftArgument + queries * costOfRightArgumentWithBinding

        long leftCard = cardinalityEstimator.getCardinality(join.getLeftArg(), source);
        long rightCard = cardinalityEstimator.getCardinality(join.getRightArg(), source);
        long joinCard = cardinalityEstimator.getCardinality(join, source);

        double commuCost =
                leftCard * (C_TRANSFER_QUERY + C_TRANSFER_TUPLE)
                + joinCard * C_TRANSFER_TUPLE;

        return getCost(join.getLeftArg(), source) + getCost(join.getRightArg(), source)  + commuCost;
    }

    public double getCost(HashJoin join, URI source) {
        long leftCard = cardinalityEstimator.getCardinality(join.getLeftArg());
        long rightCard = cardinalityEstimator.getCardinality(join.getRightArg());

        //return (leftCard + rightCard) * C_TRANSFER_TUPLE + 2 * C_TRANSFER_QUERY;
        return getCost(join.getLeftArg()) + getCost(join.getRightArg())
                + C_HASH_TUPLE*leftCard + C_PROBE_TUPLE*rightCard;
    }

    public double getCost(MergeJoin join, URI source) {
        double cost1 = getCost(join.getLeftArg(), source);
        double cost2 = getCost(join.getRightArg(), source);
        return cost1 + cost2;
    }

    public double getCost(Join join, URI source) {
        if (join instanceof BindJoin)
            return getCost((BindJoin)join, source);
        else if (join instanceof HashJoin)
            return getCost((HashJoin)join, source);
        else if (join instanceof MergeJoin)
            return getCost((MergeJoin)join, source);

        long leftCard = cardinalityEstimator.getCardinality(join.getLeftArg(), source);
        long rightCard = cardinalityEstimator.getCardinality(join.getRightArg(), source);

        return (leftCard + rightCard) * C_TRANSFER_TUPLE + 2 * C_TRANSFER_QUERY;
    }

    public double getCost(Order order, URI source){
        long card = cardinalityEstimator.getCardinality(order.getArg(), source);
        return getCost(order.getArg(), source) + card * Math.log(card);
    }

    public double getCost(UnaryTupleOperator expr, URI source) {
        return getCost(expr.getArg(), source);
    }

    public double getCost(BinaryTupleOperator expr, URI source) {
        return getCost(expr.getLeftArg(), source) + getCost(expr.getRightArg(), source);
    }
}
