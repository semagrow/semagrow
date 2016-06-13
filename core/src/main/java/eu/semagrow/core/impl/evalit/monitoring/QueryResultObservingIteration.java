package eu.semagrow.core.impl.evalit.monitoring;

import org.eclipse.rdf4j.common.iteration.Iteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryResultHandler;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;

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

            if (!initialized) {
                handler.startQueryResult(new ArrayList<String>(bindings.getBindingNames()));
                initialized = true;
            }

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
