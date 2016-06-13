package eu.semagrow.core.evalit.util;

import eu.semagrow.core.evalit.QueryExecutor;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

/**
 * Created by angel on 6/20/14.
 */
public class QueryExecutorWrapper implements QueryExecutor {

    private QueryExecutor executor;

    public QueryExecutorWrapper(QueryExecutor executor) {
        assert executor != null;
        this.executor = executor;
    }

    public QueryExecutor getWrappedExecutor() { return executor; }

    public void initialize() { getWrappedExecutor().initialize(); }

    public void shutdown() { getWrappedExecutor().shutdown(); }

    public CloseableIteration<BindingSet, QueryEvaluationException>
        evaluate(IRI endpoint, TupleExpr expr, BindingSet bindings) throws QueryEvaluationException {
        return getWrappedExecutor().evaluate(endpoint, expr, bindings);
    }

    public CloseableIteration<BindingSet, QueryEvaluationException>
        evaluate(IRI endpoint, TupleExpr expr, CloseableIteration<BindingSet, QueryEvaluationException> bindingIter) throws QueryEvaluationException {
        return getWrappedExecutor().evaluate(endpoint,expr,bindingIter);
    }
}
