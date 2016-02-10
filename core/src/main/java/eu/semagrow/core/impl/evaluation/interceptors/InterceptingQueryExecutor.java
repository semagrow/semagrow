package eu.semagrow.core.impl.evaluation.interceptors;


import eu.semagrow.core.evaluation.QueryExecutor;

/**
 * Created by angel on 6/28/14.
 */
public interface InterceptingQueryExecutor extends QueryExecutor {

    void addEvaluationInterceptor(QueryExecutionInterceptor interceptor);

    void removeEvaluationInterceptor(QueryExecutionInterceptor interceptor);
}
