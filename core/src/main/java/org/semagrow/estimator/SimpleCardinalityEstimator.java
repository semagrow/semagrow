package org.semagrow.estimator;

import org.semagrow.art.Loggable;
import org.semagrow.plan.Plan;
import org.semagrow.statistics.Statistics;
import org.semagrow.plan.operators.SourceQuery;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
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
    public BigInteger getCardinality(TupleExpr expr)  {

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
        else if (expr instanceof Distinct)
            return getCardinality((Distinct)expr);
        else if (expr instanceof Reduced)
            return getCardinality((Reduced)expr);
        else if (expr instanceof Group)
            return getCardinality((Group)expr);

        return BigInteger.ZERO;

    }

    public BigInteger getCardinality(StatementPattern pattern) {
        return BigInteger.valueOf(statistics.getStats(pattern, EmptyBindingSet.getInstance()).getCardinality());
    }

    public BigInteger getCardinality(Distinct distinct) {
        return getCardinality(distinct.getArg());
    }

    public BigInteger getCardinality(Reduced reduced) {
        return getCardinality(reduced.getArg());
    }

    public BigInteger getCardinality(Union union) {
        return getCardinality(union.getLeftArg())
                .add(getCardinality(union.getRightArg()));
    }

    public BigInteger getCardinality(Filter filter) {
        BigDecimal sel = BigDecimal.valueOf(selectivityEstimator.getConditionSelectivity(filter.getCondition(), filter.getArg()));
        return new BigDecimal(getCardinality(filter.getArg())).multiply(sel).toBigInteger();
    }

    public BigInteger getCardinality(Projection projection) {
        return getCardinality(projection.getArg());
    }

    public BigInteger getCardinality(Slice slice) {
        BigInteger card = getCardinality(slice.getArg());
        BigInteger sliceCard = BigInteger.valueOf(slice.getOffset() + slice.getLimit());
        if (card.compareTo(sliceCard) >= 0)
            return sliceCard;
        else
            return card;
    }

    public BigInteger getCardinality(Join join){

        BigInteger card1 = getCardinality(join.getLeftArg());

        BigInteger card2 = getCardinality(join.getRightArg());

        BigDecimal sel = BigDecimal.valueOf(selectivityEstimator.getJoinSelectivity(join));

        BigInteger tt = new BigDecimal(card1.multiply(card2)).multiply(sel).setScale(0, RoundingMode.CEILING).toBigInteger();

        return tt.max(BigInteger.ZERO);
    }

    public BigInteger getCardinality(LeftJoin join) {
        BigInteger card1 = getCardinality(join.getLeftArg());
        BigInteger card2 = getCardinality(join.getRightArg());

        // A left merge B is semantically equiv to (A merge B) union (A - B)

        Join dummyJoin = new Join(join.getLeftArg().clone(), join.getRightArg().clone());

        BigDecimal sel = BigDecimal.valueOf(selectivityEstimator.getJoinSelectivity(dummyJoin));

        BigInteger card1card2 = new BigDecimal(card1.multiply(card2)).multiply(sel).toBigInteger();

        BigInteger card1compl = new BigDecimal(card1).multiply(BigDecimal.ONE.subtract(sel)).toBigInteger();

        // TODO: check the second half of the equation
        return card1card2.add( BigInteger.ZERO.max(card1compl) );

    }

    public BigInteger getCardinality(SourceQuery query) {
        return cardinalityEstimatorResolver
                .resolve(query.getSite())
                .map(c -> c.getCardinality(query.getArg()))
                .orElse(BigInteger.ZERO);
    }

    public BigInteger getCardinality(EmptySet set) {
        return BigInteger.ONE;
    }


    public BigInteger getCardinality(BindingSetAssignment assignment){
        Iterator<BindingSet> it = assignment.getBindingSets().iterator();

        int i = 0;

        while (it.hasNext()) {
            it.next();
            i++;
        }

        return BigInteger.valueOf(i);
    }

}

