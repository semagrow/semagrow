package org.semagrow.plan.util;

import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Created by angel on 4/27/14.
 */
public class FilterCollector extends AbstractQueryModelVisitor<RuntimeException> {

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
