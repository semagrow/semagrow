package eu.semagrow.core.eval;

/**
 * Created by angel on 30/3/2016.
 */
public interface QueryExecutorFactory {

    String getType();

    QueryExecutorImplConfig getConfig();

    QueryExecutor getQueryExecutor(QueryExecutorImplConfig config) throws QueryExecutorConfigException;


}
