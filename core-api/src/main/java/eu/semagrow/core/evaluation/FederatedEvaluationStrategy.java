package eu.semagrow.core.evaluation;

/**
 * The interface of a federated EvaluationStrategy
 * @author Angelos Charalambidis
 */
public interface FederatedEvaluationStrategy extends EvaluationStrategy {

    /**
     * Sets the Query Executor to be used by the strategy
     * @param executor the query executor
     */
    void setQueryExecutor(QueryExecutor executor);

    /**
     * Returns the currently used Query Executor
     * @return
     */
    QueryExecutor getQueryExecutor();

    /**
     * Sets the option to include or not provenance data during the evaluation
     * @param includeProvenance true if provenance is of interest; false otherwise
     */
    void setIncludeProvenance(boolean includeProvenance);
}
