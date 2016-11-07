package org.semagrow.estimator;

import org.semagrow.art.Loggable;
import org.semagrow.plan.operators.BindJoin;
import org.semagrow.plan.operators.HashJoin;
import org.semagrow.plan.operators.MergeJoin;
import org.semagrow.plan.operators.SourceQuery;
import org.semagrow.plan.Cost;
import org.semagrow.plan.Plan;
import org.semagrow.local.LocalSite;
import org.semagrow.selector.Site;

import org.eclipse.rdf4j.query.algebra.*;

/**
 * Created by angel on 4/28/14.
 */
public class SimpleCostEstimator implements CostEstimator {

    private CostEstimatorResolver resolver;
    private CardinalityEstimator cardinalityEstimator;

    private static double C_TRANSFER_TUPLE = 50;
    private static double C_TRANSFER_QUERY = 100;

    private static double C_PROBE_TUPLE = 0.001;   //cost to probe a tuple against a hash table
    private static double C_HASH_TUPLE = 0.003;    //cost to hash a tuple to a hash table

    public SimpleCostEstimator(CostEstimatorResolver resolver, CardinalityEstimator cardinalityEstimator) {
        this.resolver = resolver;
        this.cardinalityEstimator = cardinalityEstimator;
    }

    @Loggable
    public Cost getCost(TupleExpr expr) {
        // just favor remote queries.
        if (expr instanceof SourceQuery)
            return getCost((SourceQuery)expr);
        else if (expr instanceof Join)
            return getCost((Join)expr);
        else if (expr instanceof Order)
            return getCost((Order)expr);
        else if (expr instanceof Plan)
            return ((Plan)expr).getProperties().getCost();
        else if (expr instanceof Filter)
            return getCost((Filter)expr);
        else if (expr instanceof UnaryTupleOperator)
            return getCost((UnaryTupleOperator)expr);
        else if (expr instanceof BinaryTupleOperator)
            return getCost((BinaryTupleOperator)expr);
        else if (expr instanceof BindingSetAssignment)
            return getCost((BindingSetAssignment)expr);
        else
            return new Cost(cardinalityEstimator.getCardinality(expr));
    }

    public Cost getCost(SourceQuery expr) {

        // cardinality on a specific endpoint.
        // communication cost of the endpoint * cardinality
        // processing cost + load (it may depends on the complexity of the query)

        //int count = cardinalityEstimator.getCardinality(expr);

        //totalCost = processingCost(subexpr) + communicationCost(count, source) + initializationCostOfQuery;

        double communCost = C_TRANSFER_QUERY +
                cardinalityEstimator.getCardinality(expr.getArg()) * C_TRANSFER_TUPLE;

        Cost cost = new Cost(communCost);
        return cost;
    }

    public Cost getCost(Filter filter) {
        return getCost(filter.getArg());
    }

    public Cost getCost(BindJoin join) {
        // long cardinalityOfLeft = cardinalityEstimator.getCardinality(merge.getLeftArg());
        // long costLeftArgument = estimateCost(merge.getLeftArg());
        // long cardinalityAll = cardinalityEstimator.getCardinality(merge);
        // long queries = cardinalityOfLeft / bindingsPerQuery;
        // long resultsPerQuery = cardinalityAll / queries;
        // totalCost = costLeftArgument + queries * costOfRightArgumentWithBinding

        long leftCard = cardinalityEstimator.getCardinality(join.getLeftArg());
        long rightCard = cardinalityEstimator.getCardinality(join.getRightArg());
        long joinCard = cardinalityEstimator.getCardinality(join);

        double commuCost = C_TRANSFER_QUERY +
                leftCard * (C_TRANSFER_QUERY + C_TRANSFER_TUPLE)
                + joinCard * C_TRANSFER_TUPLE;

        return getCost(join.getLeftArg()).add(new Cost(commuCost));
    }

    public Cost getCost(HashJoin join) {
        long leftCard = cardinalityEstimator.getCardinality(join.getLeftArg());
        long rightCard = cardinalityEstimator.getCardinality(join.getRightArg());

        //return (leftCard + rightCard) * C_TRANSFER_TUPLE + 2 * C_TRANSFER_QUERY;
        return Cost.cpuCost(C_HASH_TUPLE*leftCard + C_PROBE_TUPLE*rightCard);
    }

    public Cost getCost(MergeJoin join) {
        Cost cost1 = getCost(join.getLeftArg());
        Cost cost2 = getCost(join.getRightArg());
        return cost1.add(cost2);
    }

    public Cost getCost(Join join) {
        if (join instanceof BindJoin)
            return getCost((BindJoin)join);
        else if (join instanceof HashJoin)
            return getCost((HashJoin)join);
        else if (join instanceof MergeJoin)
            return getCost((MergeJoin)join);

        long leftCard = cardinalityEstimator.getCardinality(join.getLeftArg());
        long rightCard = cardinalityEstimator.getCardinality(join.getRightArg());

        return new Cost((leftCard + rightCard) * C_TRANSFER_TUPLE + 2 * C_TRANSFER_QUERY);
    }

    public Cost getCost(Order order){
        long card = cardinalityEstimator.getCardinality(order.getArg());
        return getCost(order.getArg()).add(Cost.cpuCost(card * Math.log(card)));
    }

    public Cost getCost(UnaryTupleOperator expr) {
        return getCost(expr.getArg());
    }

    public Cost getCost(BinaryTupleOperator expr) {
        return getCost(expr.getLeftArg()).add(getCost(expr.getRightArg()));
    }

    public Cost getCost(BindingSetAssignment expr) {
        return new Cost(0);
    }
}
