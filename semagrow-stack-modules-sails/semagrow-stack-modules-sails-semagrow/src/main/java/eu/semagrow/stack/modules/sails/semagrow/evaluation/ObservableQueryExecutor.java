package eu.semagrow.stack.modules.sails.semagrow.evaluation;

import eu.semagrow.stack.modules.api.evaluation.QueryExecutor;
import eu.semagrow.stack.modules.sails.semagrow.evaluation.iteration.ObservingIteration;
import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.Iteration;
import org.openrdf.model.URI;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;

/**
 * Created by angel on 6/20/14.
 */
public class ObservableQueryExecutor extends QueryExecutorWrapper {

    public ObservableQueryExecutor(QueryExecutor executor) {
        super(executor);
    }

    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException>
        evaluate(URI endpoint, TupleExpr expr, BindingSet bindings) throws QueryEvaluationException {
        return observeIteration(super.evaluate(endpoint, expr, bindings));
    }

    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(URI endpoint, TupleExpr expr, CloseableIteration<BindingSet, QueryEvaluationException> bindingIter) throws QueryEvaluationException {
        return observeIteration(super.evaluate(endpoint, expr, bindingIter));
    }

    public CloseableIteration<BindingSet, QueryEvaluationException>
        observeIteration(CloseableIteration<BindingSet, QueryEvaluationException> iter) {
        return new QueryObserver(iter);
    }

    protected class QueryObserver extends ObservingIteration<BindingSet,QueryEvaluationException> {

        public QueryObserver(Iteration<BindingSet, QueryEvaluationException> iter) {
            super(iter);
        }

        @Override
        public void observe(BindingSet bindings) {

        }

        @Override
        public void observeExceptionally(QueryEvaluationException e) {

        }
    }
}
