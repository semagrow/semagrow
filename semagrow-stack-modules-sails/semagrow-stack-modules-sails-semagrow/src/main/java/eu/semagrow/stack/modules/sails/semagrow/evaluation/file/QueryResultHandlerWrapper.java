package eu.semagrow.stack.modules.sails.semagrow.evaluation.file;

import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryResultHandler;
import org.openrdf.query.QueryResultHandlerException;
import org.openrdf.query.TupleQueryResultHandlerException;

import java.util.List;

/**
 * Created by angel on 10/20/14.
 */
public class QueryResultHandlerWrapper implements QueryResultHandler {

    private QueryResultHandler handler;

    public QueryResultHandlerWrapper(QueryResultHandler handler) {
        this.handler = handler;
    }

    @Override
    public void handleBoolean(boolean b) throws QueryResultHandlerException {
        handler.handleBoolean(b);
    }

    @Override
    public void handleLinks(List<String> strings) throws QueryResultHandlerException {
        handler.handleLinks(strings);
    }

    @Override
    public void startQueryResult(List<String> strings) throws TupleQueryResultHandlerException {
        handler.startQueryResult(strings);
    }

    @Override
    public void endQueryResult() throws TupleQueryResultHandlerException {
        handler.endQueryResult();
    }

    @Override
    public void handleSolution(BindingSet bindings) throws TupleQueryResultHandlerException {
        handler.handleSolution(bindings);
    }
}
