package eu.semagrow.stack.modules.sails.semagrow.evaluation.monitoring.querylog;

import eu.semagrow.stack.modules.sails.semagrow.evaluation.monitoring.ObservingIteration;
import info.aduna.iteration.Iteration;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryResultHandler;
import org.openrdf.query.TupleQueryResultHandlerException;

import java.util.ArrayList;

/**
 * Created by angel on 10/20/14.
 */
public class QueryResultObservingIteration extends ObservingIteration<BindingSet, QueryEvaluationException> {

    private QueryResultHandler handler;

    private boolean initialized = false;

    public QueryResultObservingIteration(QueryResultHandler handler, Iteration<BindingSet,QueryEvaluationException> iter) {
        super(iter);
        assert handler != null;
        this.handler = handler;
    }

    @Override
    public void observe(BindingSet bindings) throws QueryEvaluationException {

        try {

            if (!initialized)
                handler.startQueryResult(new ArrayList<String>(bindings.getBindingNames()));

            handler.handleSolution(bindings);

        } catch(Exception e) {
            throw new QueryEvaluationException(e);
        }
    }

    @Override
    public void observeExceptionally(QueryEvaluationException e) { }

    @Override
    public void handleClose() throws QueryEvaluationException {
        super.handleClose();

        try {
            if (initialized)
                handler.endQueryResult();
        } catch (TupleQueryResultHandlerException e) {
            throw new QueryEvaluationException(e);
        }

    }

}
