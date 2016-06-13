package eu.semagrow.core.impl.evaluation;

import eu.semagrow.core.impl.evaluation.file.MaterializationHandle;
import eu.semagrow.core.impl.evaluation.file.MaterializationManager;
import eu.semagrow.core.impl.evaluation.file.QueryResultHandlerWrapper;
import eu.semagrow.core.impl.evaluation.util.LoggingUtil;
import eu.semagrow.querylog.api.QueryLogException;
import eu.semagrow.querylog.api.QueryLogHandler;
import eu.semagrow.querylog.api.QueryLogRecord;
import eu.semagrow.querylog.impl.QueryLogRecordImpl;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
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
    private UUID uuid;
    private int count;
    private IRI endpoint;

    private long start;
    private long end;


    private QueryLogRecord queryLogRecord;


    public LoggingTupleQueryResultHandler(String q, QueryResultHandler handler, QueryLogHandler qfrHandler, MaterializationManager mat, IRI endpoint) {
        super(handler);
        this.mat = mat;
        this.qfrHandler = qfrHandler;
        this.endpoint = endpoint;
        this.query = q;
        this.uuid = UUID.randomUUID();
    }

    @Override
    public void startQueryResult(List<String> list) throws TupleQueryResultHandlerException {
        count = 0;
        start = System.currentTimeMillis();

        if (handle != null) {
            queryLogRecord = createMetadata(endpoint, query, EmptyBindingSet.getInstance(), list);
            try {
                handle = mat.saveResult();
            } catch (QueryEvaluationException e) {
                logger.error("Error while creating a materialization handle", e);
            }
            handle.startQueryResult(list);
        }
        super.startQueryResult(list);
    }

    @Override
    public void endQueryResult() throws TupleQueryResultHandlerException {
        LoggingUtil.logEnd(logger, query, endpoint, count);

        if (handle != null) {
            handle.endQueryResult();
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
        }
        super.endQueryResult();
    }

    @Override
    public void handleSolution(BindingSet bindingSet) throws TupleQueryResultHandlerException {
        count++;
        if (count == 1) {
            LoggingUtil.logFirstResult(logger, query, endpoint);
        }
        LoggingUtil.logResult(logger, query, endpoint, bindingSet);

        if (handle != null) {
            handle.handleSolution(bindingSet);
        }
        super.handleSolution(bindingSet);
    }



    protected QueryLogRecordImpl createMetadata(IRI endpoint, String expr, BindingSet bindings, List<String> bindingNames) {
        return new QueryLogRecordImpl(uuid, endpoint, expr, bindings, bindingNames);
    }

}
