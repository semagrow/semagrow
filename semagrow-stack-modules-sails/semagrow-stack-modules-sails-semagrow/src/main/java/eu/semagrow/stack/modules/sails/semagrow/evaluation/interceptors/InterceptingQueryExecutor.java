package eu.semagrow.stack.modules.sails.semagrow.evaluation.interceptors;


import eu.semagrow.stack.modules.api.evaluation.QueryExecutor;

/**
 * Created by angel on 6/28/14.
 */
public interface InterceptingQueryExecutor extends QueryExecutor {

    void addEvaluationInterceptor(QueryExecutionInterceptor interceptor);

    void removeEvaluationInterceptor(QueryExecutionInterceptor interceptor);
}
