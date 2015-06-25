package eu.semagrow.core.impl.evaluation.interceptors;

import eu.semagrow.core.evaluation.QueryEvaluationSession;

/**
 * Created by angel on 6/28/14.
 */
public interface EvaluationSessionAwareInterceptor {

    void setQueryEvaluationSession(QueryEvaluationSession session);

    QueryEvaluationSession getQueryEvaluationSession();
}
