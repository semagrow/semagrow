package org.semagrow.querylog.api;

/**
 * Created by angel on 10/22/14.
 */
public interface QueryLogWriter extends QueryLogHandler {

    void startQueryLog() throws QueryLogException;

    void endQueryLog() throws QueryLogException;

}
