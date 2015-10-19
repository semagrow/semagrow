package eu.semagrow.core.impl.evaluation.rx;

import eu.semagrow.core.impl.evaluation.file.MaterializationHandle;
import eu.semagrow.core.impl.evaluation.file.MaterializationManager;
import eu.semagrow.core.impl.evaluation.file.QueryResultHandlerWrapper;
import eu.semagrow.querylog.api.QueryLogException;
import eu.semagrow.querylog.api.QueryLogHandler;
import eu.semagrow.querylog.api.QueryLogRecord;
import eu.semagrow.querylog.impl.QueryLogRecordImpl;
import org.openrdf.model.URI;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.*;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.impl.EmptyBindingSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

/**
 * Created by antru on 6/10/2015.
 */
public class LoggingTupleQueryResultHandler extends QueryResultHandlerWrapper implements TupleQueryResultHandler {

    private static final Logger logger = LoggerFactory.getLogger(LoggingTupleQueryResultHandler.class);

    private MaterializationManager mat;
    private MaterializationHandle handle;

    private QueryLogHandler qfrHandler;

    private String query;
    private String id;
    private UUID uuid;
    private int count;

    private long start;
    private long end;


    private QueryLogRecord queryLogRecord;


    public LoggingTupleQueryResultHandler(String q, QueryResultHandler handler, QueryLogHandler qfrHandler, MaterializationManager mat) {
        super(handler);
        this.mat = mat;
        this.qfrHandler = qfrHandler;

        query = q;
        uuid = UUID.randomUUID();
        if (uuid.toString().length() > 12) {
            id = uuid.toString().substring(uuid.toString().length() - 12);
        }
        else {
            id = uuid.toString();
        }
    }

    @Override
    public void startQueryResult(List<String> list) throws TupleQueryResultHandlerException {
        count = 0;
        logger.debug("{} - Starting {}", id, query.replace("\n", " "));
        start = System.currentTimeMillis();

        queryLogRecord = createMetadata(ValueFactoryImpl.getInstance().createURI("http://www.iamabsoluteuri.com", ""), query, EmptyBindingSet.getInstance(), list);
        try {
            handle = mat.saveResult();
        } catch (QueryEvaluationException e) {
            logger.error("Error while creating a materialization handle", e);
        }
        handle.startQueryResult(list);
        super.startQueryResult(list);
    }

    @Override
    public void endQueryResult() throws TupleQueryResultHandlerException {
        handle.endQueryResult();
        logger.debug("{} - Query returned {} results.", id, count);

        end = System.currentTimeMillis();
        queryLogRecord.setCardinality(count);
        queryLogRecord.setDuration(start, end);

        if (queryLogRecord.getCardinality() == 0) {
            try {
                handle.destroy();
            } catch (IOException e) {
                logger.error("Error while destroying a materialization handle", e);
            }
        } else {
            queryLogRecord.setResults(handle.getId());
        }

        try {
            qfrHandler.handleQueryRecord(queryLogRecord);
        } catch (QueryLogException e) {
            logger.error("Error while pushing record to queryloghandler", e);
        }

        super.endQueryResult();
    }

    @Override
    public void handleSolution(BindingSet bindingSet) throws TupleQueryResultHandlerException {
        count++;
        logger.debug("{} - Found {}", id, bindingSet);
        handle.handleSolution(bindingSet);
        super.handleSolution(bindingSet);
    }



    protected QueryLogRecordImpl createMetadata(URI endpoint, String expr, BindingSet bindings, List<String> bindingNames) {
        return new QueryLogRecordImpl(uuid, endpoint, expr, bindings, bindingNames);
    }
}
