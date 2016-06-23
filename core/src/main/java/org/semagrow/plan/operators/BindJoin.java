package org.semagrow.plan.operators;

import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

/**
 * Will evaluate the result of the leftArg and
 * substitute the bindings to the rightArg.
 * The right argument must be of a certain time (e.g. SourceQuery, Service etc)
 */
public class BindJoin extends Join {

    public BindJoin(TupleExpr e1, TupleExpr e2) {
        super(e1,e2);
    }

    @Override
    public int hashCode() {
        return "bind".hashCode() + super.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof BindJoin) {
            BindJoin j = (BindJoin) o;
            return getLeftArg().equals(j.getLeftArg()) && getRightArg().equals(j.getRightArg());
        }
        return false;
    }
}
