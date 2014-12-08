package eu.semagrow.stack.modules.sails.semagrow.evaluation.interceptors;

import eu.semagrow.stack.modules.api.evaluation.QueryEvaluationSession;

/**
 * Created by angel on 6/28/14.
 */
public interface EvaluationSessionAwareInterceptor {

    void setQueryEvaluationSession(QueryEvaluationSession session);

    QueryEvaluationSession getQueryEvaluationSession();
}
