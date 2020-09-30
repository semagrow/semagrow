package org.semagrow.geospatial.execution;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.util.QueryOptimizerList;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.reactivestreams.Publisher;
import org.semagrow.connector.sparql.execution.SPARQLQueryExecutor;
import org.semagrow.evaluation.file.MaterializationManager;
import org.semagrow.querylog.api.QueryLogHandler;
import org.semagrow.selector.Site;

import java.util.List;

public class GeoSPARQLQueryExecutor extends SPARQLQueryExecutor {

    public GeoSPARQLQueryExecutor(QueryLogHandler qfrHandler, MaterializationManager mat) {
        super(qfrHandler, mat);
    }

    public Publisher<BindingSet> evaluate(final Site endpoint, final TupleExpr expr, final BindingSet bindings)
            throws QueryEvaluationException
    {
        optimize(expr, bindings);
        return super.evaluate(endpoint, expr, bindings);
    }

    public Publisher<BindingSet> evaluate(final Site endpoint, final TupleExpr expr, final List<BindingSet> bindingList)
            throws QueryEvaluationException
    {
        optimize(expr, bindingList);
        return super.evaluate(endpoint, expr, bindingList);

    }

    protected void optimize(TupleExpr expr, BindingSet bindings) {
        QueryOptimizer queryOptimizer =  new QueryOptimizerList(
                new BBoxDistanceOptimizer()
        );
        queryOptimizer.optimize(expr, null, bindings);
    }

    protected void optimize(TupleExpr expr, List<BindingSet> bindingSetList) {
        if (bindingSetList.isEmpty()) {
            optimize(expr, new EmptyBindingSet());
        }
        else {
            if (bindingSetList.size() == 1) {
                optimize(expr, bindingSetList.get(0));
            }
        }
    }
}
