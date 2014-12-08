package eu.semagrow.stack.modules.sails.semagrow.evaluation.interceptors;

import eu.semagrow.stack.modules.api.evaluation.QueryExecutor;
import eu.semagrow.stack.modules.sails.semagrow.evaluation.base.QueryExecutorWrapper;
import info.aduna.iteration.CloseableIteration;

import org.openrdf.model.URI;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by angel on 6/28/14.
 */
public class InterceptingQueryExecutorWrapper extends QueryExecutorWrapper
    implements InterceptingQueryExecutor {

    private List<QueryExecutionInterceptor> interceptors = new LinkedList<QueryExecutionInterceptor>();

    public InterceptingQueryExecutorWrapper(QueryExecutor executor) {
        super(executor);
    }

    public void addEvaluationInterceptor(QueryExecutionInterceptor interceptor) {
        if (!interceptors.contains(interceptor))
            interceptors.add(interceptor);
    }

    public void removeEvaluationInterceptor(QueryExecutionInterceptor interceptor) {
        if (interceptors.contains(interceptor))
            interceptors.remove(interceptor);
    }

    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException>
        evaluate(URI endpoint, TupleExpr expr, BindingSet bindings)
            throws QueryEvaluationException {

        CloseableIteration<BindingSet, QueryEvaluationException> result = super.evaluate(endpoint, expr, bindings);

        if (!interceptors.isEmpty()) {
            for (QueryExecutionInterceptor interceptor : interceptors)
                result = interceptor.afterExecution(endpoint, expr, bindings, result);
        }
        return result;
    }

    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException>
        evaluate(URI endpoint, TupleExpr expr, CloseableIteration<BindingSet, QueryEvaluationException> bindingIter)
            throws QueryEvaluationException
    {
        CloseableIteration<BindingSet, QueryEvaluationException> result =
                super.evaluate(endpoint, expr, bindingIter);

        if (!interceptors.isEmpty()) {
            for (QueryExecutionInterceptor interceptor : interceptors)
                result = interceptor.afterExecution(endpoint, expr, bindingIter, result);
        }

        return result;
    }

}
