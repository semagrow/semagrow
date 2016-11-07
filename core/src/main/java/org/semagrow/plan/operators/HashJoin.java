package org.semagrow.plan.operators;

import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

/**
 * Hash join is a join implementation that hash the result set
 * of the left operand and probe each result tuple of the right operand
 * against that hash table.
 * @author acharal
 */
public class HashJoin extends Join {

    public HashJoin(TupleExpr e1, TupleExpr e2) {
        super(e1,e2);
    }

    @Override
    public int hashCode() {
        return "hash".hashCode() + super.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof HashJoin) {
            HashJoin j = (HashJoin) o;
            return getLeftArg().equals(j.getLeftArg()) && getRightArg().equals(j.getRightArg());
        }
        return false;
    }
}
