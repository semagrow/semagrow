package eu.semagrow.core.impl.estimator;

import eu.semagrow.art.Loggable;
import eu.semagrow.core.estimator.CardinalityEstimator;
import eu.semagrow.core.estimator.SelectivityEstimator;
import eu.semagrow.core.impl.planner.Plan;
import eu.semagrow.core.statistics.Statistics;
import eu.semagrow.core.statistics.StatisticsProvider;
import eu.semagrow.commons.algebra.SourceQuery;

import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.*;
import org.openrdf.query.algebra.helpers.VarNameCollector;
import org.openrdf.query.impl.EmptyBindingSet;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by angel on 4/28/14.
 */
public class CardinalityEstimatorImpl implements CardinalityEstimator, SelectivityEstimator {

    private StatisticsProvider statistics;

    public CardinalityEstimatorImpl(StatisticsProvider statistics) {
        this.statistics = statistics;
    }

    @Loggable
    public long getCardinality(TupleExpr expr)
    {
        return getCardinality(expr, null);
    }

    public long getCardinality(TupleExpr expr, URI source)  {

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
        else if (expr instanceof EmptySet)
            return getCardinality((EmptySet)expr, source);
        else if (expr instanceof BindingSetAssignment)
            return getCardinality((BindingSetAssignment)expr, source);
        else if (expr instanceof Plan)
            return ((Plan)expr).getProperties().getCardinality();

        return 0;

    }

    public long getCardinality(StatementPattern pattern, URI source) {

        if (source != null)
            return statistics.getStats(pattern, EmptyBindingSet.getInstance(), source).getCardinality();
        else
            return 0;

    }

    public long getCardinality(Union union, URI source) {
        return getCardinality(union.getLeftArg(), source) +
               getCardinality(union.getRightArg(), source);
    }

    public long getCardinality(Filter filter, URI source) {
        double sel = getConditionSelectivity(filter.getCondition(), filter.getArg(), source);
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

        long card1 = getCardinality(join.getLeftArg(), source);

        long card2 = getCardinality(join.getRightArg(), source);

        double sel = getJoinSelectivity(join, source);

        double t = card1 * card2 * sel;
        long tt = (long)Math.ceil(t);

        if (tt < 0)
            return 0;

        return tt;
    }

    public long getCardinality(LeftJoin join, URI source) {
        long card1 = getCardinality(join.getLeftArg(), source);
        long card2 = getCardinality(join.getRightArg(), source);

        // A left merge B is semantically equiv to (A merge B) union (A - B)

        Join dummyJoin = new Join(join.getLeftArg().clone(), join.getRightArg().clone());
        double sel = getJoinSelectivity(dummyJoin, source);

        // TODO: check the second half of the equation
        return (long)(card1 * card2 * sel) + (long)(card1 * (1/sel));
    }

    public long getCardinality(SourceQuery query, URI source) {
        long card = 0;
        for (URI src : query.getSources())
            card += getCardinality(query.getArg(), src);
        return card;
    }

    public long getCardinality(EmptySet set, URI source) {
        return 0;
    }


    public long getCardinality(BindingSetAssignment assignment, URI source){
        Iterator<BindingSet> it = assignment.getBindingSets().iterator();

        int i = 0;

        while (it.hasNext()) {
            it.next();
            i++;
        }

        return i;
    }

    /**
     * Estimate the merge selectivity factor *sel* of a merge, such that
     * merge cardinality = cross product cardinality * sel.
     * @param join the merge expression
     * @param source a referring data source
     * @return the selectivity factor
     */
    public double getJoinSelectivity(Join join, URI source) {

        Set<String> varNames = getCommonVariables(join.getLeftArg(), join.getRightArg());
        // TODO: check if calculation of selectivity of multiple variables is correct.
        // TODO: consult page 6 of http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.129.3162&rep=rep1&type=pdf
        double sel = 1;
        for (String var : varNames)
            sel *= getVarSelectivity(var, join, source);

        return sel;
    }

    public double getJoinSelectivity(Join join) {
        return getJoinSelectivity(join, null);
    }

    /**
     * Compute the reduction factor if a condition is applied in the expression, i.e.
     * cardinality after the condition application = cardinality before the condition application * reduction factor.
     * @param condition
     * @param expr
     * @param source
     * @return
     */
    public double getConditionSelectivity(ValueExpr condition, TupleExpr expr, URI source) {
        if (condition instanceof And) {
            return getConditionSelectivity((And)condition, expr, source);
        } else if (condition instanceof Or) {
            return getConditionSelectivity((Or)condition, expr, source);
        } else if (condition instanceof Not) {
            return getConditionSelectivity((Not)condition, expr, source);
        }
        // else identify ranges?
        //return 0.0001;
        return 0.25;
    }

    public double getConditionSelectivity(And valueExpr, TupleExpr expr, URI source) {
        double sel1 = getConditionSelectivity(valueExpr.getLeftArg(), expr, source);
        double sel2 = getConditionSelectivity(valueExpr.getRightArg(), expr, source);
        return sel1 * sel2;
    }

    public double getConditionSelectivity(Or valueExpr, TupleExpr expr, URI source) {
        double sel1 = getConditionSelectivity(valueExpr.getLeftArg(), expr, source);
        double sel2 = getConditionSelectivity(valueExpr.getRightArg(), expr, source);
        return sel1 + sel2 - sel1 * sel2;
    }

    public double getConditionSelectivity(Not valueExpr, TupleExpr expr, URI source) {
        double sel = getConditionSelectivity(valueExpr.getArg(), expr, source);
        return 1 - sel;
    }

    public double getConditionSelectivity(Compare valueExpr, TupleExpr expr, URI source) {
        Compare.CompareOp op = valueExpr.getOperator();
        return 0.5;
    }

    public double getConditionSelectivity(ValueExpr condition, TupleExpr expr) {
        return getConditionSelectivity(condition, expr, null);
    }

    public double getVarSelectivity(String varName, TupleExpr expr, URI source) {
        if (expr instanceof StatementPattern)
            return getVarSelectivity(varName, (StatementPattern)expr, source);
        else if (expr instanceof BinaryTupleOperator)
            return getVarSelectivity(varName, (BinaryTupleOperator)expr, source);
        else if (expr instanceof UnaryTupleOperator)
            return getVarSelectivity(varName, (UnaryTupleOperator)expr, source);
        else if (expr instanceof BindingSetAssignment)
            return getVarSelectivity(varName, (BindingSetAssignment)expr, source);

        Set<String> varNames = VarNameCollector.process(expr);

        if (!varNames.contains(varName))
            return 1;

        return 0.5;
    }

    public double getVarSelectivity(String varName, StatementPattern pattern, URI source) {

        long distinct = getVarCardinality(varName, pattern, source);

        return ((double)1/distinct);
    }

    public double getVarSelectivity(String varName, BindingSetAssignment assignment, URI source) {
        if (assignment.getBindingNames().contains(varName))
            return 1;
        else {
            // count distinct;
            long distinct = 0;
            Set<Value> distinctValues = new HashSet<>();

            Iterator<BindingSet> it = assignment.getBindingSets().iterator();

            while (it.hasNext())
            {
                BindingSet bset = it.next();
                Value v = bset.getValue(varName);
                if (v != null)
                    distinctValues.add(v);
            }

            distinct = (long)distinctValues.size();
            return ((double)1/distinct);
        }
    }

    public double getVarSelectivity(String varName, SourceQuery expr, URI source) {
        double sel = 1;
        for (URI src : expr.getSources()) {
            sel = Math.min(sel, getVarSelectivity(varName, expr.getArg(), src));
        }
        return sel;
    }

    public double getVarSelectivity(String varName, Plan p, URI source) {
        return getVarSelectivity(varName, p.getArg(), p.getProperties().getSite().getURI());
    }

    public double getVarSelectivity(String varName, UnaryTupleOperator expr, URI source) {
        if (expr instanceof SourceQuery)
            return getVarSelectivity(varName, (SourceQuery)expr, source);
        else if (expr instanceof Plan) {
            return getVarSelectivity(varName, (Plan)expr, source);
        } else
            return getVarSelectivity(varName, expr.getArg(), source);
    }

    public double getVarSelectivity(String varName, BinaryTupleOperator expr, URI source) {
        double leftSel = getVarSelectivity(varName, expr.getLeftArg(), source);
        double rightSel = getVarSelectivity(varName, expr.getRightArg(), source);
        return Math.min(leftSel, rightSel);
    }

    /**
     * Estimate the number of distinct values of a given variable.
     * @param varName the name of the variable
     * @param pattern the triple pattern
     * @param source a potential referring data source
     * @return the estimated number of distinct values of a variable.
     */
    public long getVarCardinality(String varName, StatementPattern pattern, URI source) {

        Statistics stats = statistics.getStats(pattern, EmptyBindingSet.getInstance(), source);

        return stats.getVarCardinality(varName);
    }


    /*
    public long getVarCardinality(String varName, Union union, URI source) {
        long card1 = getVarCardinality(varName, union.getLeftArg(), source);
        long card2 = getVarCardinality(varName, union.getRightArg(), source);
        // if (subset) then max else if (disjoint) sum else something in between
        return Math.max(card1,card2);
    }

    public long getVarCardinality(String varName, Join merge, URI source) {
        long card1 = getVarCardinality(varName, merge.getLeftArg(), source);
        long card2 = getVarCardinality(varName, merge.getRightArg(), source);
        // if joined variable then
        // if (subset) then min else if (disjoint) 0 else something in between
        // if not joined variable then
        // if in left arg ->
    }
    */

    // helper
    private Set<String> getCommonVariables(TupleExpr expr1, TupleExpr expr2) {
        Set<String> set1 = VarNameCollector.process(expr1);
        Set<String> set2 = VarNameCollector.process(expr2);
        set1.retainAll(set2);
        return set1;
    }


}

