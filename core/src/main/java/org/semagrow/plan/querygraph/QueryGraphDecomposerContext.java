package org.semagrow.plan.querygraph;

import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.semagrow.plan.DecomposerContext;

/**
 * Created by angel on 23/6/2016.
 */
public class QueryGraphDecomposerContext extends DecomposerContext {

    private QueryGraph graph;

    protected QueryGraphDecomposerContext(TupleExpr expr) {
        super(expr);
        graph = QueryGraph.create(expr);
    }

    public QueryGraph getQueryGraph() { return graph; }

}
