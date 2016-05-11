package eu.semagrow.core.evalit;

/**
 * Created by angel on 7/1/14.
 */
public interface FederatedQueryEvaluationSession extends QueryEvaluationSession {

    FederatedEvaluationStrategy getEvaluationStrategy();

    QueryExecutor getQueryExecutor();
}
