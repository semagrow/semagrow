package eu.semagrow.stack.modules.sails.semagrow.estimator;

import eu.semagrow.stack.modules.api.ResourceSelector;
import eu.semagrow.stack.modules.querydecomp.Statistics;
import eu.semagrow.stack.modules.querydecomp.estimator.CardinalityEstimator;
import eu.semagrow.stack.modules.sails.semagrow.algebra.SourceQuery;
import org.openrdf.model.URI;
import org.openrdf.query.algebra.*;
import org.openrdf.query.algebra.helpers.VarNameCollector;

import java.util.Set;

/**
 * Created by angel on 4/28/14.
 */
public class CardinalityEstimatorImpl implements CardinalityEstimator {

    private Statistics statistics;

    public CardinalityEstimatorImpl(Statistics statistics) {
        this.statistics = statistics;
    }

    public long getCardinality(TupleExpr expr) {

        return getCardinality(expr, null);
    }

    public long getCardinality(TupleExpr expr, URI source) {
        if (expr instanceof StatementPattern)
            return getCardinality((StatementPattern)expr, source);
        else if (expr instanceof Union)
            return getCardinality((Union)expr, source);
        else if (expr instanceof Filter)
            return getCardinality((Filter)expr, source);
        else if (expr instanceof Projection)
            return getCardinality((Projection)expr, source);
        else if (expr instanceof Slice)
            return getCardinality((Slice)expr, source);
        else if (expr instanceof Join)
            return getCardinality((Join)expr, source);
        else if (expr instanceof LeftJoin)
            return getCardinality((LeftJoin)expr, source);
        else if (expr instanceof SourceQuery)
            return getCardinality((SourceQuery)expr, source);

        return 0;
    }

    public long getCardinality(StatementPattern pattern, URI source) {
        if (source != null)
            return statistics.getPatternCount(pattern, source);
        else
            return 0;
    }

    public long getCardinality(Union union, URI source) {
        return getCardinality(union.getLeftArg(), source) +
               getCardinality(union.getRightArg(), source);
    }

    public long getCardinality(Filter filter, URI source) {
        double sel = getConditionSelectivity(filter.getCondition(), source);
        return (long) (getCardinality(filter.getArg(), source) * sel);
    }

    public long getCardinality(Projection projection, URI source) {
        return getCardinality(projection.getArg(), source);
    }

    public long getCardinality(Slice slice, URI source) {
        long card = getCardinality(slice.getArg(), source);
        long sliceCard = slice.getOffset() + slice.getLimit();
        return (card > sliceCard)? sliceCard : card;
    }

    public long getCardinality(Join join, URI source){
        Set<String> varNames = getCommonVariables(join.getLeftArg(), join.getRightArg());
        long card1 = getCardinality(join.getLeftArg(), source);
        long card2 = getCardinality(join.getRightArg(), source);

        // TODO: check if calculation of selectivity of multiple variables is correct.
        // TODO: consult page 6 of http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.129.3162&rep=rep1&type=pdf
        double sel = 1;
        for (String var : varNames)
            sel *= getVarSelectivity(var, join, source);

        return (long)(card1 * card2 * sel);
    }

    public long getCardinality(LeftJoin join, URI source) {
        Set<String> varNames = getCommonVariables(join.getLeftArg(), join.getRightArg());
        long card1 = getCardinality(join.getLeftArg(), source);
        long card2 = getCardinality(join.getRightArg(), source);

        // TODO: check if calculation of selectivity of multiple variables is correct.
        // TODO: consult page 6 of http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.129.3162&rep=rep1&type=pdf
        double sel = 1;
        for (String var : varNames)
            sel *= getVarSelectivity(var, join, source);

        // TODO: check the second half of the equation
        return (long)(card1 * card2 * sel) + (long)(card1 * (1/sel));
    }

    public long getCardinality(SourceQuery query, URI source) {
        long card = 0;
        for (URI src : query.getSources())
            card += getCardinality(query.getArg(), src);
        return card;
    }

    private double getConditionSelectivity(ValueExpr condition, URI source) {
        return 1/2;
    }

    public double getVarSelectivity(String varName, TupleExpr expr, URI source) {
        return 1/2;
    }

    protected double getSubjectSel(StatementPattern pattern, URI source) {
        if (pattern.getSubjectVar().hasValue()) {
            long p = statistics.getPatternCount(pattern, source);
            long s = statistics.getDistinctSubjects(pattern, source);
            return p / s;
        }
        return 1;
    }

    protected double getObjectSel(StatementPattern pattern, URI source) {
        if (pattern.getObjectVar().hasValue()) {
            // create a new pattern with object unbound.
            StatementPattern pattern1 =
                    new StatementPattern(pattern.getSubjectVar(),
                            pattern.getPredicateVar(),
                            pattern.getObjectVar());
            long p = statistics.getPatternCount(pattern1, source);
            long o = statistics.getDistinctObjects(pattern1, source);
            return p / o;
        }
        return 1;
    }

    protected double getPredicateSel(StatementPattern pattern, URI source) {
        if (pattern.getPredicateVar().hasValue()) {
            long p = statistics.getPatternCount(pattern, source);
            long s = statistics.getDistinctPredicates(pattern, source);
            return p / s;
        }
        return 1;
    }

    // helper
    private Set<String> getCommonVariables(TupleExpr expr1, TupleExpr expr2) {
        Set<String> set1 = VarNameCollector.process(expr1);
        Set<String> set2 = VarNameCollector.process(expr2);
        set1.retainAll(set2);
        return set1;
    }
}
