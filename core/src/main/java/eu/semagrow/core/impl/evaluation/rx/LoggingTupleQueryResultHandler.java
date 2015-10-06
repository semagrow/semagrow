package eu.semagrow.core.impl.evaluation.rx;

import org.openrdf.query.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

/**
 * Created by antru on 6/10/2015.
 */
public class LoggingTupleQueryResultHandler implements TupleQueryResultHandler {

    private static final Logger logger = LoggerFactory.getLogger(LoggingTupleQueryResultHandler.class);

    private TupleQueryResultHandler wrapperHandler;
    private TupleQuery query;
    private String id;
    private int count;

    public LoggingTupleQueryResultHandler(TupleQuery q, TupleQueryResultHandler handler) {
        wrapperHandler = handler;
        query = q;
        UUID uuid = UUID.randomUUID();
        if (uuid.toString().length() > 12) {
            id = uuid.toString().substring(uuid.toString().length() - 12);
        }
        else {
            id = uuid.toString();
        }
    }

    @Override
    public void handleBoolean(boolean b) throws QueryResultHandlerException {
        wrapperHandler.handleBoolean(b);
    }

    @Override
    public void handleLinks(List<String> list) throws QueryResultHandlerException {
        wrapperHandler.handleLinks(list);
    }

    @Override
    public void startQueryResult(List<String> list) throws TupleQueryResultHandlerException {
        wrapperHandler.startQueryResult(list);
        count = 0;
        logger.debug("{} - Starting {}", id, query.toString().replace("\n", " "));
    }

    @Override
    public void endQueryResult() throws TupleQueryResultHandlerException {
        wrapperHandler.endQueryResult();
        logger.debug("{} - Query returned {} results.", id, count);
    }

    @Override
    public void handleSolution(BindingSet bindingSet) throws TupleQueryResultHandlerException {
        wrapperHandler.handleSolution(bindingSet);
        count++;
        logger.debug("{} - Found {}", id, bindingSet);
    }
}
