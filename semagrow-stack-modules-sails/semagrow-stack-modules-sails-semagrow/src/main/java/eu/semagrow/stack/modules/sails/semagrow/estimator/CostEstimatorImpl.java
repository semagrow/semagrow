package eu.semagrow.stack.modules.sails.semagrow.estimator;

import eu.semagrow.stack.modules.api.estimator.CardinalityEstimator;
import eu.semagrow.stack.modules.sails.semagrow.algebra.BindJoin;
import eu.semagrow.stack.modules.sails.semagrow.algebra.HashJoin;
import eu.semagrow.stack.modules.sails.semagrow.algebra.MergeJoin;
import eu.semagrow.stack.modules.sails.semagrow.algebra.SourceQuery;
import eu.semagrow.stack.modules.sails.semagrow.planner.Cost;
import eu.semagrow.stack.modules.sails.semagrow.planner.Plan;
import eu.semagrow.stack.modules.sails.semagrow.planner.Site;
import org.openrdf.model.URI;
import org.openrdf.query.algebra.*;

/**
 * Created by angel on 4/28/14.
 */
public class CostEstimatorImpl implements CostEstimator {

    private CardinalityEstimator cardinalityEstimator;

    private static double C_TRANSFER_TUPLE = 0.1;
    private static double C_TRANSFER_QUERY = 50;

    private static double C_PROBE_TUPLE = 0.001;   //cost to probe a tuple against a hash table
    private static double C_HASH_TUPLE = 0.003;    //cost to hash a tuple to a hash table

    public CostEstimatorImpl(CardinalityEstimator cardinalityEstimator) {
        this.cardinalityEstimator = cardinalityEstimator;
    }

    /**
     * @param expr
     * @return
     */
    public Cost getCost(TupleExpr expr) {
        return getCost(expr, Site.LOCAL);
    }

    public Cost getCost(TupleExpr expr, Site site) {
        return getCost(expr, site.getURI());
    }

    public Cost getCost(TupleExpr expr, URI source) {
        // just favor remote queries.
        if (expr instanceof SourceQuery)
            return getCost((SourceQuery)expr, source);
        else if (expr instanceof Join)
            return getCost((Join)expr, source);
        else if (expr instanceof Order)
            return getCost((Order)expr, source);
        else if (expr instanceof Plan)
            return ((Plan)expr).getProperties().getCost();
        else
            return new Cost(cardinalityEstimator.getCardinality(expr, source));
    }

    public Cost getCost(SourceQuery expr, URI source) {

        // cardinality on a specific endpoint.
        // communication cost of the endpoint * cardinality
        // processing cost + load (it may depends on the complexity of the query)

        //int count = cardinalityEstimator.getCardinality(expr);

        //totalCost = processingCost(subexpr) + communicationCost(count, source) + initializationCostOfQuery;

        double communCost = C_TRANSFER_QUERY +
                cardinalityEstimator.getCardinality(expr.getArg()) * C_TRANSFER_TUPLE;

        Cost cost = getCost(expr.getArg()).add(new Cost(communCost));

        return cost;
    }

    public Cost getCost(BindJoin join, URI source) {
        // long cardinalityOfLeft = cardinalityEstimator.getCardinality(join.getLeftArg());
        // long costLeftArgument = estimateCost(join.getLeftArg());
        // long cardinalityAll = cardinalityEstimator.getCardinality(join);
        // long queries = cardinalityOfLeft / bindingsPerQuery;
        // long resultsPerQuery = cardinalityAll / queries;
        // totalCost = costLeftArgument + queries * costOfRightArgumentWithBinding

        long leftCard = cardinalityEstimator.getCardinality(join.getLeftArg(), source);
        long rightCard = cardinalityEstimator.getCardinality(join.getRightArg(), source);
        long joinCard = cardinalityEstimator.getCardinality(join, source);

        double commuCost = C_TRANSFER_QUERY +
                leftCard * (C_TRANSFER_QUERY + C_TRANSFER_TUPLE)
                + joinCard * C_TRANSFER_TUPLE;

        return getCost(join.getLeftArg()).add(new Cost(commuCost));
    }

    public Cost getCost(HashJoin join, URI source) {
        long leftCard = cardinalityEstimator.getCardinality(join.getLeftArg());
        long rightCard = cardinalityEstimator.getCardinality(join.getRightArg());

        //return (leftCard + rightCard) * C_TRANSFER_TUPLE + 2 * C_TRANSFER_QUERY;
        return Cost.cpuCost(C_HASH_TUPLE*leftCard + C_PROBE_TUPLE*rightCard);
    }

    public Cost getCost(MergeJoin join, URI source) {
        Cost cost1 = getCost(join.getLeftArg(), source);
        Cost cost2 = getCost(join.getRightArg(), source);
        return cost1.add(cost2);
    }

    public Cost getCost(Join join, URI source) {
        if (join instanceof BindJoin)
            return getCost((BindJoin)join, source);
        else if (join instanceof HashJoin)
            return getCost((HashJoin)join, source);
        else if (join instanceof MergeJoin)
            return getCost((MergeJoin)join, source);

        long leftCard = cardinalityEstimator.getCardinality(join.getLeftArg(), source);
        long rightCard = cardinalityEstimator.getCardinality(join.getRightArg(), source);

        return new Cost((leftCard + rightCard) * C_TRANSFER_TUPLE + 2 * C_TRANSFER_QUERY);
    }

    public Cost getCost(Order order, URI source){
        long card = cardinalityEstimator.getCardinality(order.getArg(), source);
        return getCost(order.getArg(), source).add(Cost.cpuCost(card * Math.log(card)));
    }

    public Cost getCost(UnaryTupleOperator expr, URI source) {
        return getCost(expr.getArg(), source);
    }

    public Cost getCost(BinaryTupleOperator expr, URI source) {
        return getCost(expr.getLeftArg(), source).add(getCost(expr.getRightArg(), source));
    }
}
