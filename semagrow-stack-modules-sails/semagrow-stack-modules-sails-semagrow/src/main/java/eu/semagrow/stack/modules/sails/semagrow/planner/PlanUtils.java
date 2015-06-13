package eu.semagrow.stack.modules.sails.semagrow.planner;

import eu.semagrow.stack.modules.sails.semagrow.helpers.FilterCollector;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.helpers.VarNameCollector;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Set;

/**
 * Created by angel on 22/4/2015.
 */
public class PlanUtils {


    public static Collection<ValueExpr> getRelevantFiltersConditions(TupleExpr e, Collection<ValueExpr> filterConditions) {
        Set<String> variables = VarNameCollector.process(e);
        Collection<ValueExpr> relevantConditions = new LinkedList<ValueExpr>();

        for (ValueExpr condition : filterConditions) {
            Set<String> conditionVariables = VarNameCollector.process(condition);
            if (variables.containsAll(conditionVariables))
                relevantConditions.add(condition);
        }

        return relevantConditions;
    }

    private static TupleExpr applyFilters(TupleExpr e, Collection<ValueExpr> conditions) {
        TupleExpr expr = e;

        for (ValueExpr condition : conditions)
            expr = new Filter(expr, condition);

        return expr;
    }

    public static TupleExpr applyRemainingFilters(TupleExpr e, Collection<ValueExpr> conditions) {
        Collection<ValueExpr> filtersApplied = FilterCollector.process(e);
        Collection<ValueExpr> remainingFilters = getRelevantFiltersConditions(e, conditions);
        remainingFilters.removeAll(filtersApplied);
        return applyFilters(e, remainingFilters);
    }

}
