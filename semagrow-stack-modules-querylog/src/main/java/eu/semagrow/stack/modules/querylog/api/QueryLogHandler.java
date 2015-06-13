package eu.semagrow.stack.modules.querylog.api;

/**
 * Created by angel on 10/20/14.
 */
public interface QueryLogHandler {

    void handleQueryRecord(QueryLogRecord queryLogRecord) throws QueryLogException;

}
