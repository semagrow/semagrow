package org.semagrow.querylog.config;

import org.semagrow.querylog.api.QueryLogException;
import org.semagrow.querylog.api.QueryLogWriter;


/**
 * Created by angel on 10/21/14.
 */
public interface QueryLogFactory {

    QueryLogWriter getQueryRecordLogger(QueryLogConfig config) throws QueryLogException;

}