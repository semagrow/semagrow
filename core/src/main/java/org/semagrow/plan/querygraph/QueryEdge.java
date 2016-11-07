package org.semagrow.plan.querygraph;

import org.eclipse.rdf4j.query.algebra.TupleExpr;

/**
 * Created by angel on 18/5/2015.
 */
class QueryEdge {

    private TupleExpr from;

    private TupleExpr to;

    private QueryPredicate predicate;

    public QueryEdge(TupleExpr from, TupleExpr to, QueryPredicate predicate) {
        this.from = from;
        this.to = to;
        this.predicate = predicate;
    }

    public TupleExpr getFrom() { return from; }

    public TupleExpr getTo() { return to; }

    public QueryPredicate getPredicate() { return predicate; }
}