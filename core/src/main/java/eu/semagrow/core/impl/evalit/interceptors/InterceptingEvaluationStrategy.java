package eu.semagrow.core.impl.evalit.interceptors;

import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;

/**
 * Created by angel on 6/27/14.
 */
public interface InterceptingEvaluationStrategy extends EvaluationStrategy {

    void addEvaluationInterceptor(QueryEvaluationInterceptor interceptor);

    void removeEvaluationInterceptor(QueryEvaluationInterceptor interceptor);

}
