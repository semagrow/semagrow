package org.semagrow.plan.optimizer;

import org.eclipse.rdf4j.query.algebra.TupleExpr;

@Deprecated
public interface DisjointCheker {
    boolean areDisjoint(TupleExpr expr1, TupleExpr expr2);
}
