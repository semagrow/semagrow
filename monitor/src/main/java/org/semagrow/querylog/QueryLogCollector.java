package org.semagrow.querylog;

import org.semagrow.querylog.api.QueryLogException;
import org.semagrow.querylog.api.QueryLogRecord;
import org.semagrow.querylog.api.QueryLogWriter;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Created by angel on 10/22/14.
 */
public class QueryLogCollector implements QueryLogWriter {

    private Collection<QueryLogRecord> collection;

    public QueryLogCollector(Collection<QueryLogRecord> collection) { this.collection = collection; }

    @Override
    public void startQueryLog() throws QueryLogException {
        collection = new LinkedList<QueryLogRecord>();
    }

    @Override
    public void handleQueryRecord(QueryLogRecord queryLogRecord) throws QueryLogException {
        collection.add(queryLogRecord);
    }

    @Override
    public void endQueryLog() throws QueryLogException { }

}
