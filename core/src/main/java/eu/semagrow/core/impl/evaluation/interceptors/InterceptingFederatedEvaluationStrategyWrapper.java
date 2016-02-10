package eu.semagrow.core.impl.evaluation.interceptors;

import eu.semagrow.core.evaluation.FederatedEvaluationStrategy;
import eu.semagrow.core.impl.evaluation.FederatedEvaluationStrategyWrapper;
import info.aduna.iteration.CloseableIteration;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;

import java.util.Collections;
import java.util.List;

/**
 * Created by angel on 6/27/14.
 */
public class InterceptingFederatedEvaluationStrategyWrapper
        extends FederatedEvaluationStrategyWrapper
        implements InterceptingEvaluationStrategy {

    private List<QueryEvaluationInterceptor> interceptors = Collections.emptyList();

    public InterceptingFederatedEvaluationStrategyWrapper(FederatedEvaluationStrategy wrapped) {
        super(wrapped);
    }

    public void addEvaluationInterceptor(QueryEvaluationInterceptor interceptor) {
        if (!interceptors.contains(interceptor))
            interceptors.add(interceptor);
    }

    public void removeEvaluationInterceptor(QueryEvaluationInterceptor interceptor) {
        if (interceptors.contains(interceptor))
            interceptors.remove(interceptor);
    }


    @Override
    public CloseableIteration<BindingSet,QueryEvaluationException>
        evaluate(TupleExpr expr, BindingSet bindings) throws QueryEvaluationException {

        CloseableIteration<BindingSet,QueryEvaluationException> result = super.evaluate(expr,bindings);

        if (!interceptors.isEmpty()) {
            for (QueryEvaluationInterceptor interceptor : interceptors)
                result = interceptor.afterEvaluation(expr, bindings, result);
        }

        return result;
    }
}
