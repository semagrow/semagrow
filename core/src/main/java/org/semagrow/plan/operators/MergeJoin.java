package org.semagrow.plan.operators;

import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

/**
 * Merge join assumes that the results of its operands are ordered with
 * respect to their common variables. The result of the merge join is also
 * ordered with respect to those variables.
 * @author acharal
 */
public class MergeJoin extends Join {

    public MergeJoin(TupleExpr e1, TupleExpr e2) {
        super(e1,e2);
    }

    @Override
    public int hashCode() {
        return "mergejoin".hashCode() + super.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof MergeJoin) {
            MergeJoin j = (MergeJoin) o;
            return getLeftArg().equals(j.getLeftArg()) && getRightArg().equals(j.getRightArg());
        }
        return false;
    }
}