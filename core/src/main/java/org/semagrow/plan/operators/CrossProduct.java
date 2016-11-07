package org.semagrow.plan.operators;

import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

/**
 * Created by angel on 30/9/2016.
 */
public class CrossProduct extends Join {

    public CrossProduct(TupleExpr e1, TupleExpr e2) {
        super(e1,e2);
    }

    @Override
    public int hashCode() {
        return "cross".hashCode() + super.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof CrossProduct) {
            CrossProduct j = (CrossProduct) o;
            return getLeftArg().equals(j.getLeftArg()) && getRightArg().equals(j.getRightArg());
        }
        return false;
    }
}
