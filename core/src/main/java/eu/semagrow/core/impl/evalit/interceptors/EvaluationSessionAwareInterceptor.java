package eu.semagrow.core.impl.evalit.interceptors;

import eu.semagrow.core.evalit.QueryEvaluationSession;

/**
 * Created by angel on 6/28/14.
 */
public interface EvaluationSessionAwareInterceptor {

    void setQueryEvaluationSession(QueryEvaluationSession session);

    QueryEvaluationSession getQueryEvaluationSession();
}
