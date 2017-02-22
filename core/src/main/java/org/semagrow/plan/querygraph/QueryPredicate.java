package org.semagrow.plan.querygraph;


import org.eclipse.rdf4j.query.algebra.TupleExpr;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by angel on 18/5/2015.
 */
class QueryPredicate {

    private Set<TupleExpr> eel = new HashSet<TupleExpr>();

    public void setEEL(Set<TupleExpr> eligibleList) {
        this.eel = eligibleList;
    }

    public boolean canBeApplied(Set<TupleExpr> relations)
    {
        Set<TupleExpr> rel = new HashSet<TupleExpr>(eel);
        rel.removeAll(relations);
        return rel.isEmpty();
    }
}