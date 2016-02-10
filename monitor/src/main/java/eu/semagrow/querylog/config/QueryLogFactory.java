package eu.semagrow.querylog.config;

import eu.semagrow.querylog.api.QueryLogException;
import eu.semagrow.querylog.api.QueryLogWriter;


/**
 * Created by angel on 10/21/14.
 */
public interface QueryLogFactory {

    QueryLogWriter getQueryRecordLogger(QueryLogConfig config) throws QueryLogException;

}