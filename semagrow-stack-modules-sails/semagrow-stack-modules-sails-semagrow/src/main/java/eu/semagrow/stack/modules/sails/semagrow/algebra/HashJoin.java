package eu.semagrow.stack.modules.sails.semagrow.algebra;

import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.TupleExpr;

/**
 * Created by angel on 4/30/14.
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
