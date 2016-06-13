package eu.semagrow.core.impl.evalit.interceptors;

import eu.semagrow.core.evalit.util.QueryExecutorWrapper;
import eu.semagrow.core.evalit.QueryExecutor;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

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
        evaluate(IRI endpoint, TupleExpr expr, BindingSet bindings)
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
        evaluate(IRI endpoint, TupleExpr expr, CloseableIteration<BindingSet, QueryEvaluationException> bindingIter)
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
