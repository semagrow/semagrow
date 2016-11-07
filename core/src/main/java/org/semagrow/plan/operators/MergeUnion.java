package org.semagrow.plan.operators;

import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Union;

/**
 * Merge union is an implementation of the {@link Union} operator that
 * assumes result sets of their operands of the same ordering
 * and also produces as output an ordered result set with the same ordering.
 *
 * @see org.eclipse.rdf4j.query.algebra.Union
 * @author acharal
 */
public class MergeUnion extends Union {

    public MergeUnion(TupleExpr leftArg, TupleExpr rightArg) {
        super(leftArg, rightArg);
    }

    @Override
    public int hashCode() {
        return "mergeunion".hashCode() + super.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof MergeUnion) {
            MergeUnion j = (MergeUnion) o;
            return getLeftArg().equals(j.getLeftArg()) && getRightArg().equals(j.getRightArg());
        }
        return false;
    }
}
