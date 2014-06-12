package eu.semagrow.stack.modules.sails.semagrow.evaluation;

import eu.semagrow.stack.modules.api.evaluation.EvaluationStrategy;
import eu.semagrow.stack.modules.api.evaluation.QueryEvaluation;
import eu.semagrow.stack.modules.api.evaluation.QueryEvaluationSession;

/**
 * Created by angel on 6/11/14.
 */
public class QueryEvaluationImpl implements QueryEvaluation {

    public QueryEvaluationSession createSession() {
        return new QueryEvaluationSessionImpl();
    }

    protected class QueryEvaluationSessionImpl extends QueryEvaluationSessionImplBase {

        protected EvaluationStrategy getEvaluationStrategyInternal() {
            return new EvaluationStrategyImpl(new QueryExecutorImpl());
        }
    }
}
