package org.semagrow.plan.operators;

import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

public class BindNotExists extends Join {

    public BindNotExists(TupleExpr e1, TupleExpr e2) {
        super(e1,e2);
    }

    @Override
    public int hashCode() {
        return "bind-not-exists".hashCode() + super.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof BindNotExists) {
            BindNotExists j = (BindNotExists) o;
            return getLeftArg().equals(j.getLeftArg()) && getRightArg().equals(j.getRightArg());
        }
        return false;
    }
}

