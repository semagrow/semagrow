package eu.semagrow.commons.algebra;

import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.TupleExpr;

/**
 * Created by angel on 10/2/14.
 */
public class MergeJoin extends Join {

    public MergeJoin(TupleExpr e1, TupleExpr e2) {
        super(e1,e2);
    }

    @Override
    public int hashCode() {
        return "hash".hashCode() + super.hashCode();
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