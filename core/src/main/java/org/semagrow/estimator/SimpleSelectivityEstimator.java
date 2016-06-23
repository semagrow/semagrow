package org.semagrow.estimator;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.helpers.VarNameCollector;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.semagrow.plan.Plan;
import org.semagrow.plan.operators.SourceQuery;
import org.semagrow.statistics.StatsItem;
import org.semagrow.statistics.Statistics;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by angel on 15/6/2016.
 */
public class SimpleSelectivityEstimator implements SelectivityEstimator {

    private SelectivityEstimatorResolver resolver;
    private Statistics statistics;

    public SimpleSelectivityEstimator(SelectivityEstimatorResolver resolver,
                                      Statistics statistics) {

        this.resolver = resolver;
        this.statistics = statistics;
    }

    /**
     * Estimate the merge selectivity factor *sel* of a merge, such that
     * merge cardinality = cross product cardinality * sel.
     * @param join the merge expression
     * @return the selectivity factor
     */
    public double getJoinSelectivity(Join join) {

        Set<String> varNames = getCommonVariables(join.getLeftArg(), join.getRightArg());
        // TODO: check if calculation of selectivity of multiple variables is correct.
        // TODO: consult page 6 of http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.129.3162&rep=rep1&type=pdf
        double sel = 1;
        for (String var : varNames)
            sel *= getVarSelectivity(var, join);

        return sel;
    }


    /**
     * Compute the reduction factor if a condition is applied in the expression, i.e.
     * cardinality after the condition application = cardinality before the condition application * reduction factor.
     * @param condition
     * @param expr
     * @return
     */
    public double getConditionSelectivity(ValueExpr condition, TupleExpr expr) {
        if (condition instanceof And) {
            return getConditionSelectivity((And)condition, expr);
        } else if (condition instanceof Or) {
            return getConditionSelectivity((Or)condition, expr);
        } else if (condition instanceof Not) {
            return getConditionSelectivity((Not)condition, expr);
        }
        // else identify ranges?
        //return 0.0001;
        return 0.25;
    }

    public double getConditionSelectivity(And valueExpr, TupleExpr expr) {
        double sel1 = getConditionSelectivity(valueExpr.getLeftArg(), expr);
        double sel2 = getConditionSelectivity(valueExpr.getRightArg(), expr);
        return sel1 * sel2;
    }

    public double getConditionSelectivity(Or valueExpr, TupleExpr expr) {
        double sel1 = getConditionSelectivity(valueExpr.getLeftArg(), expr);
        double sel2 = getConditionSelectivity(valueExpr.getRightArg(), expr);
        return sel1 + sel2 - sel1 * sel2;
    }

    public double getConditionSelectivity(Not valueExpr, TupleExpr expr) {
        double sel = getConditionSelectivity(valueExpr.getArg(), expr);
        return 1 - sel;
    }

    public double getConditionSelectivity(Compare valueExpr, TupleExpr expr) {
        Compare.CompareOp op = valueExpr.getOperator();
        return 0.5;
    }


    public double getVarSelectivity(String varName, TupleExpr expr) {
        if (expr instanceof StatementPattern)
            return getVarSelectivity(varName, (StatementPattern)expr);
        else if (expr instanceof BinaryTupleOperator)
            return getVarSelectivity(varName, (BinaryTupleOperator)expr);
        else if (expr instanceof UnaryTupleOperator)
            return getVarSelectivity(varName, (UnaryTupleOperator)expr);
        else if (expr instanceof BindingSetAssignment)
            return getVarSelectivity(varName, (BindingSetAssignment)expr);

        Set<String> varNames = VarNameCollector.process(expr);

        if (!varNames.contains(varName))
            return 1;

        return 0.5;
    }

    public double getVarSelectivity(String varName, StatementPattern pattern) {

        long distinct = getVarCardinality(varName, pattern);

        return ((double)1/distinct);
    }

    public double getVarSelectivity(String varName, BindingSetAssignment assignment) {
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

    public double getVarSelectivity(String varName, SourceQuery expr) {
        return resolver.resolve(expr.getSite())
                .map( est -> est.getVarSelectivity(varName, expr.getArg()))
                .orElse(1.0);
    }

    public double getVarSelectivity(String varName, Plan p) {
        return resolver.resolve(p.getProperties().getSite())
                .map( est -> est.getVarSelectivity(varName, p.getArg()))
                .orElse(1.0);
    }

    public double getVarSelectivity(String varName, UnaryTupleOperator expr) {
        if (expr instanceof SourceQuery)
            return getVarSelectivity(varName, (SourceQuery)expr);
        else if (expr instanceof Plan) {
            return getVarSelectivity(varName, (Plan)expr);
        } else
            return getVarSelectivity(varName, expr.getArg());
    }

    public double getVarSelectivity(String varName, BinaryTupleOperator expr) {
        double leftSel = getVarSelectivity(varName, expr.getLeftArg());
        double rightSel = getVarSelectivity(varName, expr.getRightArg());
        return Math.min(leftSel, rightSel);
    }

    /**
     * Estimate the number of distinct values of a given variable.
     * @param varName the name of the variable
     * @param pattern the triple pattern
     * @return the estimated number of distinct values of a variable.
     */
    public long getVarCardinality(String varName, StatementPattern pattern) {

        StatsItem stats = statistics.getStats(pattern, EmptyBindingSet.getInstance());

        return stats.getVarCardinality(varName);
    }

    // helper
    private Set<String> getCommonVariables(TupleExpr expr1, TupleExpr expr2) {
        Set<String> set1 = VarNameCollector.process(expr1);
        Set<String> set2 = VarNameCollector.process(expr2);
        set1.retainAll(set2);
        return set1;
    }


}
