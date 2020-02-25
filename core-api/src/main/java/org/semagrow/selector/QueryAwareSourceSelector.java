package org.semagrow.selector;

import org.eclipse.rdf4j.query.algebra.TupleExpr;

public interface QueryAwareSourceSelector extends SourceSelector {

    void processTupleExpr(TupleExpr expr);

}
