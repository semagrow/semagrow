package org.semagrow.estimator;

import org.semagrow.art.Loggable;
import org.semagrow.plan.Plan;
import org.semagrow.statistics.Statistics;
import org.semagrow.plan.operators.SourceQuery;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;

import java.util.Iterator;

/**
 * Created by angel on 4/28/14.
 */
public class SimpleCardinalityEstimator implements CardinalityEstimator {

    private CardinalityEstimatorResolver cardinalityEstimatorResolver;
    private SelectivityEstimator selectivityEstimator;
    private Statistics statistics;

    public SimpleCardinalityEstimator(CardinalityEstimatorResolver cardinalityEstimatorResolver,
                                      SelectivityEstimator selectivityEstimator,
                                      Statistics statistics)
    {
        this.cardinalityEstimatorResolver = cardinalityEstimatorResolver;
        this.selectivityEstimator = selectivityEstimator;
        this.statistics = statistics;
    }

    @Loggable
    public long getCardinality(TupleExpr expr)  {

        if (expr instanceof StatementPattern)
            return getCardinality((StatementPattern)expr);
        else if (expr instanceof Union)
            return getCardinality((Union)expr);
        else if (expr instanceof Filter)
            return getCardinality((Filter)expr);
        else if (expr instanceof Projection)
            return getCardinality((Projection)expr);
        else if (expr instanceof Slice)
            return getCardinality((Slice)expr);
        else if (expr instanceof Join)
            return getCardinality((Join)expr);
        else if (expr instanceof LeftJoin)
            return getCardinality((LeftJoin)expr);
        else if (expr instanceof SourceQuery)
            return getCardinality((SourceQuery)expr);
        else if (expr instanceof EmptySet)
            return getCardinality((EmptySet)expr);
        else if (expr instanceof BindingSetAssignment)
            return getCardinality((BindingSetAssignment)expr);
        else if (expr instanceof Plan)
            return ((Plan)expr).getProperties().getCardinality();

        return 0;

    }

    public long getCardinality(StatementPattern pattern) {

        return statistics.getStats(pattern, EmptyBindingSet.getInstance()).getCardinality();

    }

    public long getCardinality(Union union) {
        return getCardinality(union.getLeftArg()) +
               getCardinality(union.getRightArg());
    }

    public long getCardinality(Filter filter) {
        double sel = selectivityEstimator.getConditionSelectivity(filter.getCondition(), filter.getArg());
        return (long) (getCardinality(filter.getArg()) * sel);
    }

    public long getCardinality(Projection projection) {
        return getCardinality(projection.getArg());
    }

    public long getCardinality(Slice slice) {
        long card = getCardinality(slice.getArg());
        long sliceCard = slice.getOffset() + slice.getLimit();
        return (card > sliceCard)? sliceCard : card;
    }

    public long getCardinality(Join join){

        long card1 = getCardinality(join.getLeftArg());

        long card2 = getCardinality(join.getRightArg());

        double sel = selectivityEstimator.getJoinSelectivity(join);

        double t = card1 * card2 * sel;
        long tt = (long)Math.ceil(t);

        if (tt < 0)
            return 0;

        return tt;
    }

    public long getCardinality(LeftJoin join) {
        long card1 = getCardinality(join.getLeftArg());
        long card2 = getCardinality(join.getRightArg());

        // A left merge B is semantically equiv to (A merge B) union (A - B)

        Join dummyJoin = new Join(join.getLeftArg().clone(), join.getRightArg().clone());
        double sel = selectivityEstimator.getJoinSelectivity(dummyJoin);

        // TODO: check the second half of the equation
        return (long)(card1 * card2 * sel) + (long)Math.max(0, card1*(1 - sel));

    }

    public long getCardinality(SourceQuery query) {
        return cardinalityEstimatorResolver
                .resolve(query.getSite())
                .map(c -> c.getCardinality(query.getArg()))
                .orElse((long)0);
    }

    public long getCardinality(EmptySet set) {
        return 0;
    }


    public long getCardinality(BindingSetAssignment assignment){
        Iterator<BindingSet> it = assignment.getBindingSets().iterator();

        int i = 0;

        while (it.hasNext()) {
            it.next();
            i++;
        }

        return i;
    }

}

