package eu.semagrow.core.impl.evalit.interceptors;

import eu.semagrow.core.evalit.FederatedEvaluationStrategy;
import eu.semagrow.core.evalit.util.FederatedEvaluationStrategyWrapper;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

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
