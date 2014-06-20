package eu.semagrow.stack.modules.sails.semagrow.evaluation;

import eu.semagrow.stack.modules.api.evaluation.QueryExecutor;
import info.aduna.iteration.CloseableIteration;
import org.openrdf.model.URI;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;

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
        evaluate(URI endpoint, TupleExpr expr, BindingSet bindings) throws QueryEvaluationException {
        return getWrappedExecutor().evaluate(endpoint, expr, bindings);
    }

    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(URI endpoint, TupleExpr expr, CloseableIteration<BindingSet, QueryEvaluationException> bindingIter) throws QueryEvaluationException {
        return getWrappedExecutor().evaluate(endpoint,expr,bindingIter);
    }
}
