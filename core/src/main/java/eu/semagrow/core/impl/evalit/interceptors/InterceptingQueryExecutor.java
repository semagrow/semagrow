package eu.semagrow.core.impl.evalit.interceptors;


import eu.semagrow.core.evalit.QueryExecutor;

/**
 * Created by angel on 6/28/14.
 */
public interface InterceptingQueryExecutor extends QueryExecutor {

    void addEvaluationInterceptor(QueryExecutionInterceptor interceptor);

    void removeEvaluationInterceptor(QueryExecutionInterceptor interceptor);
}
