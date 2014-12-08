package eu.semagrow.stack.modules.sails.semagrow.evaluation.interceptors;

import eu.semagrow.stack.modules.api.evaluation.QueryEvaluationSession;

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
