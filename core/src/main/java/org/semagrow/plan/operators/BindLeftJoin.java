package org.semagrow.plan.operators;

import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.ValueExpr;

/**
 * Created by angel on 5/5/2017.
 */
public class BindLeftJoin  extends LeftJoin {

    public BindLeftJoin(TupleExpr e1, TupleExpr e2) {
        super(e1,e2);
    }

    public BindLeftJoin(TupleExpr leftArg, TupleExpr rightArg, ValueExpr condition) {
        this(leftArg, rightArg);
        this.setCondition(condition);
    }

    @Override
    public int hashCode() {
        return "bind-left".hashCode() + super.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof BindLeftJoin) {
            BindLeftJoin j = (BindLeftJoin) o;
            return getLeftArg().equals(j.getLeftArg()) && getRightArg().equals(j.getRightArg());
        }
        return false;
    }
}
