package eu.semagrow.stack.modules.sails.semagrow.helpers;

import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by angel on 4/27/14.
 */
public class FilterCollector extends QueryModelVisitorBase<RuntimeException> {

    private Collection<ValueExpr> filters;

    private FilterCollector() {
        filters = new LinkedList<ValueExpr>();
    }

    public static Collection<ValueExpr> process(TupleExpr expr){
        FilterCollector filterCollector = new FilterCollector();
        expr.visit(filterCollector);
        return filterCollector.filters;
    }

    public void meet(Filter filter) {
        filters.add(filter.getCondition());
        filter.getArg().visit(this);
    }
}
