package eu.semagrow.core.evaluation;

/**
 * Created by angel on 6/27/14.
 */
public interface FederatedEvaluationStrategy extends EvaluationStrategy {

    void setQueryExecutor(QueryExecutor executor);

    QueryExecutor getQueryExecutor();

    void setIncludeProvenance(boolean includeProvenance);
}
