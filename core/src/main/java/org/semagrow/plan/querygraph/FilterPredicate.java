package org.semagrow.plan.querygraph;

import org.eclipse.rdf4j.query.algebra.ValueExpr;

/**
 * Created by angel on 23/6/2016.
 */
public class FilterPredicate extends QueryPredicate {

    public FilterPredicate(ValueExpr valueExpr) {
        this.valueExpr = valueExpr;
    }

    private ValueExpr valueExpr;

}
