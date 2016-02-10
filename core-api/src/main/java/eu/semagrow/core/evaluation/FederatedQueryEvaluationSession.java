package eu.semagrow.core.evaluation;

/**
 * Created by angel on 7/1/14.
 */
public interface FederatedQueryEvaluationSession extends QueryEvaluationSession {

    FederatedEvaluationStrategy getEvaluationStrategy();

    QueryExecutor getQueryExecutor();
}
