package org.semagrow.plan.operators;

import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

/**
 * BindJoin is a king of join implementation similar to the Nested Loop Join.
 * Evaluates the left operand and then evaluates the right operand given the
 * result set of the left as {@link org.eclipse.rdf4j.query.BindingSet}s.
 * <p />
 * The right argument must be of a certain type (e.g. {@link SourceQuery},
 * {@link org.eclipse.rdf4j.query.algebra.Service}).
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
