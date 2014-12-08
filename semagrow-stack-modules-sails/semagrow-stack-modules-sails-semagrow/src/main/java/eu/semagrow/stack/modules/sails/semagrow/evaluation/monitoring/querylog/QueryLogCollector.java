package eu.semagrow.stack.modules.sails.semagrow.evaluation.monitoring.querylog;

import java.util.Collection;

/**
 * Created by angel on 10/22/14.
 */
public class QueryLogCollector implements QueryLogHandler {

    private Collection<QueryLogRecord> collection;

    public QueryLogCollector(Collection<QueryLogRecord> collection) {
        assert collection != null;
        this.collection = collection;
    }

    @Override
    public void handleQueryRecord(QueryLogRecord queryLogRecord) throws QueryLogException {
        collection.add(queryLogRecord);
    }

}
