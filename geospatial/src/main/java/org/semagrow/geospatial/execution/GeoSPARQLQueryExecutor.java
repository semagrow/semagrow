package org.semagrow.geospatial.execution;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.reactivestreams.Publisher;
import org.semagrow.connector.sparql.SPARQLSite;
import org.semagrow.connector.sparql.execution.SPARQLQueryExecutor;
import org.semagrow.evaluation.file.MaterializationManager;
import org.semagrow.geospatial.site.GeoSPARQLSite;
import org.semagrow.querylog.api.QueryLogHandler;
import org.semagrow.selector.Site;
import reactor.core.publisher.Flux;

import java.util.List;

public class GeoSPARQLQueryExecutor extends SPARQLQueryExecutor {

    private BBoxDistanceOptimizer distanceOptimizer = new BBoxDistanceOptimizer();

    public GeoSPARQLQueryExecutor(QueryLogHandler qfrHandler, MaterializationManager mat) {
        super(qfrHandler, mat);
    }

    /* reactive api */

    @Override
    public Publisher<BindingSet> evaluate(final Site endpoint, final TupleExpr expr, final BindingSet bindings)
            throws QueryEvaluationException
    {
        return evaluateReactorImpl((GeoSPARQLSite) endpoint, expr, bindings);
    }

    @Override
    public Publisher<BindingSet> evaluate(final Site endpoint, final TupleExpr expr, final List<BindingSet> bindingsList)
            throws QueryEvaluationException
    {
        return evaluateReactorImpl((GeoSPARQLSite) endpoint, expr, bindingsList);
    }

    /* evaluate using reaactor */

    public Flux<BindingSet> evaluateReactorImpl(GeoSPARQLSite endpoint, TupleExpr expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        if (bindings.size() == 0) {
            return evaluateReactorImpl((SPARQLSite) endpoint, expr, bindings);
        }
        else {
            distanceOptimizer.optimize(expr, null, bindings);
            BindingSet bindingsExt = distanceOptimizer.expandBindings(bindings);

            return evaluateReactorImpl((SPARQLSite) endpoint, expr, bindingsExt)
                    .map(b -> bindingSetOps.project(bindings.getBindingNames(), b));
        }
    }

    protected Flux<BindingSet> evaluateReactorImpl(GeoSPARQLSite endpoint, TupleExpr expr, List<BindingSet> bindingsList)
            throws QueryEvaluationException
    {
        if (bindingsList.isEmpty()) {
            return evaluateReactorImpl((SPARQLSite) endpoint, expr, bindingsList);
        }
        else {
            BindingSet template =  bindingsList.get(0);
            TupleExpr clone = expr.clone();
            distanceOptimizer.optimize(clone, null, template);
            List<BindingSet> bindingsListExt = distanceOptimizer.expandBindings(bindingsList);

            return evaluateReactorImpl((SPARQLSite) endpoint, clone, bindingsListExt)
                    .map(b -> bindingSetOps.project(template.getBindingNames(), b));
        }
    }
}
