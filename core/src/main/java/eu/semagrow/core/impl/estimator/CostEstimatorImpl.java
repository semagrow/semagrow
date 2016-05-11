package eu.semagrow.core.impl.estimator;

import eu.semagrow.art.Loggable;
import eu.semagrow.core.estimator.CardinalityEstimator;
import eu.semagrow.core.impl.plan.ops.BindJoin;
import eu.semagrow.core.impl.plan.ops.HashJoin;
import eu.semagrow.core.impl.plan.ops.MergeJoin;
import eu.semagrow.core.impl.plan.ops.SourceQuery;
import eu.semagrow.core.estimator.CostEstimator;
import eu.semagrow.core.plan.Cost;
import eu.semagrow.core.plan.Plan;
import eu.semagrow.core.source.LocalSite;
import eu.semagrow.core.source.Site;

import org.openrdf.model.URI;
import org.openrdf.query.algebra.*;

/**
 * Created by angel on 4/28/14.
 */
public class CostEstimatorImpl implements CostEstimator {

    private CardinalityEstimator cardinalityEstimator;

    private static double C_TRANSFER_TUPLE = 50;
    private static double C_TRANSFER_QUERY = 100;

    private static double C_PROBE_TUPLE = 0.001;   //cost to probe a tuple against a hash table
    private static double C_HASH_TUPLE = 0.003;    //cost to hash a tuple to a hash table

    public CostEstimatorImpl(CardinalityEstimator cardinalityEstimator) {
        this.cardinalityEstimator = cardinalityEstimator;
    }

    @Loggable
    public Cost getCost(TupleExpr expr) {
        return getCost(expr, LocalSite.getInstance());
    }


    @Loggable
    public Cost getCost(TupleExpr expr, Site source) {
        // just favor remote queries.
        if (expr instanceof SourceQuery)
            return getCost((SourceQuery)expr, source);
        else if (expr instanceof Join)
            return getCost((Join)expr, source);
        else if (expr instanceof Order)
            return getCost((Order)expr, source);
        else if (expr instanceof Plan)
            return ((Plan)expr).getProperties().getCost();
        else if (expr instanceof Filter)
            return getCost((Filter)expr, source);
        else if (expr instanceof UnaryTupleOperator)
            return getCost((UnaryTupleOperator)expr, source);
        else if (expr instanceof BinaryTupleOperator)
            return getCost((BinaryTupleOperator)expr, source);
        else if (expr instanceof BindingSetAssignment)
            return getCost((BindingSetAssignment)expr, source);
        else
            return new Cost(cardinalityEstimator.getCardinality(expr, source));
    }

    public Cost getCost(SourceQuery expr, Site source) {

        // cardinality on a specific endpoint.
        // communication cost of the endpoint * cardinality
        // processing cost + load (it may depends on the complexity of the query)

        //int count = cardinalityEstimator.getCardinality(expr);

        //totalCost = processingCost(subexpr) + communicationCost(count, source) + initializationCostOfQuery;

        double communCost = C_TRANSFER_QUERY +
                cardinalityEstimator.getCardinality(expr.getArg()) * C_TRANSFER_TUPLE;

        Cost cost = getCost(expr.getArg()).add(new Cost(communCost));
        cost = new Cost(communCost);
        return cost;
    }

    public Cost getCost(Filter filter, Site source) {
        return getCost(filter.getArg(), source);
    }

    public Cost getCost(BindJoin join, Site source) {
        // long cardinalityOfLeft = cardinalityEstimator.getCardinality(merge.getLeftArg());
        // long costLeftArgument = estimateCost(merge.getLeftArg());
        // long cardinalityAll = cardinalityEstimator.getCardinality(merge);
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

    public Cost getCost(HashJoin join, Site source) {
        long leftCard = cardinalityEstimator.getCardinality(join.getLeftArg());
        long rightCard = cardinalityEstimator.getCardinality(join.getRightArg());

        //return (leftCard + rightCard) * C_TRANSFER_TUPLE + 2 * C_TRANSFER_QUERY;
        return Cost.cpuCost(C_HASH_TUPLE*leftCard + C_PROBE_TUPLE*rightCard);
    }

    public Cost getCost(MergeJoin join, Site source) {
        Cost cost1 = getCost(join.getLeftArg(), source);
        Cost cost2 = getCost(join.getRightArg(), source);
        return cost1.add(cost2);
    }

    public Cost getCost(Join join, Site source) {
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

    public Cost getCost(Order order, Site source){
        long card = cardinalityEstimator.getCardinality(order.getArg(), source);
        return getCost(order.getArg(), source).add(Cost.cpuCost(card * Math.log(card)));
    }

    public Cost getCost(UnaryTupleOperator expr, Site source) {
        return getCost(expr.getArg(), source);
    }

    public Cost getCost(BinaryTupleOperator expr, Site source) {
        return getCost(expr.getLeftArg(), source).add(getCost(expr.getRightArg(), source));
    }

    public Cost getCost(BindingSetAssignment expr, Site source) {
        return new Cost(0);
    }
}
