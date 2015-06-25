package eu.semagrow.querylog.api;

/**
 * Created by angel on 10/21/14.
 */
public interface QueryLogFactory {

    QueryLogWriter getQueryRecordLogger(QueryLogConfig config) throws QueryLogException;

}