package eu.semagrow.core.impl.evalit.interceptors;

import eu.semagrow.core.evalit.QueryEvaluationSession;

/**
 * Created by angel on 6/28/14.
 */
public class AbstractEvaluationSessionAwareInterceptor
    implements EvaluationSessionAwareInterceptor
{

    private QueryEvaluationSession session;

    public void setQueryEvaluationSession(QueryEvaluationSession session) { this.session = session; }

    public QueryEvaluationSession getQueryEvaluationSession() { return session; }
}
