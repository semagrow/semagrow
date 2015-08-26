package eu.semagrow.commons.algebra;

import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Union;

/**
 * Created by angel on 10/2/14.
 */
public class Merge extends Union {

    public Merge(TupleExpr leftArg, TupleExpr rightArg) {
        super(leftArg, rightArg);
    }

    @Override
    public int hashCode() {
        return "hash".hashCode() + super.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Merge) {
            Merge j = (Merge) o;
            return getLeftArg().equals(j.getLeftArg()) && getRightArg().equals(j.getRightArg());
        }
        return false;
    }
}
