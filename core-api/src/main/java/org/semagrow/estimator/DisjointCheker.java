package org.semagrow.estimator;

import org.eclipse.rdf4j.query.algebra.TupleExpr;

public interface DisjointCheker {
    boolean areDisjoint(TupleExpr expr1, TupleExpr expr2);
}
