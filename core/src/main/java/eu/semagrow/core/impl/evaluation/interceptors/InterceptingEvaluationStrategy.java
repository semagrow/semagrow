package eu.semagrow.core.impl.evaluation.interceptors;

import org.openrdf.query.algebra.evaluation.EvaluationStrategy;

/**
 * Created by angel on 6/27/14.
 */
public interface InterceptingEvaluationStrategy extends EvaluationStrategy {

    void addEvaluationInterceptor(QueryEvaluationInterceptor interceptor);

    void removeEvaluationInterceptor(QueryEvaluationInterceptor interceptor);

}
