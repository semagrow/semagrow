package eu.semagrow.stack.modules.sails.semagrow.evaluation.monitoring.querylog;

import eu.semagrow.stack.modules.sails.semagrow.evaluation.file.MaterializationHandle;
import eu.semagrow.stack.modules.sails.semagrow.evaluation.file.MaterializationManager;
import eu.semagrow.stack.modules.sails.semagrow.evaluation.interceptors.AbstractEvaluationSessionAwareInterceptor;
import eu.semagrow.stack.modules.sails.semagrow.evaluation.interceptors.QueryExecutionInterceptor;
import eu.semagrow.stack.modules.sails.semagrow.evaluation.monitoring.MeasuringIteration;
import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.DelayedIteration;
import info.aduna.iteration.Iteration;
import info.aduna.iteration.Iterations;
import org.openrdf.model.URI;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by angel on 10/20/14.
 */
public class QueryLogInterceptor
        extends AbstractEvaluationSessionAwareInterceptor
        implements QueryExecutionInterceptor {

    private MaterializationManager fileManager;

    private QueryLogHandler qfrHandler;

    private QueryLogRecordFactory qlrf;

    public QueryLogInterceptor(QueryLogRecordFactory queryLogRecordFactory,
                               QueryLogHandler qfrHandler,
                               MaterializationManager fileManager) {
        this.qfrHandler = qfrHandler;
        this.fileManager = fileManager;
        this.qlrf = queryLogRecordFactory;
    }

    public MaterializationManager getFileManager() {
        return fileManager;
    }

    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException>
        afterExecution(URI endpoint, TupleExpr expr, BindingSet bindings, CloseableIteration<BindingSet, QueryEvaluationException> result) {

        QueryLogRecord metadata = createMetadata(endpoint, expr, bindings.getBindingNames());
        return observe(metadata, result);
    }

    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException>
        afterExecution(URI endpoint, TupleExpr expr, CloseableIteration<BindingSet, QueryEvaluationException> bindingIter, CloseableIteration<BindingSet, QueryEvaluationException> result)
    {

        List<BindingSet> bindings = Collections.<BindingSet>emptyList();

        try {
            bindings = Iterations.asList(bindingIter);
        } catch (QueryEvaluationException e) {
            //?
        }

//        bindingIter = new CollectionIteration<BindingSet, QueryEvaluationException>(bindings);

        Set<String> bindingNames = (bindings.size() == 0) ? new HashSet<String>() : bindings.get(0).getBindingNames();

        QueryLogRecord metadata = createMetadata(endpoint, expr, bindingNames);

        return observe(metadata, result);
    }

    protected QueryLogRecord createMetadata(URI endpoint, TupleExpr expr, Set<String> bindingNames) {

        return qlrf.createQueryLogRecord(endpoint, expr, bindingNames);
        //return new QueryLogRecordImpl(this.getQueryEvaluationSession(), endpoint, expr, bindingNames);
    }

    protected CloseableIteration<BindingSet, QueryEvaluationException>
        observe(QueryLogRecord metadata, CloseableIteration<BindingSet, QueryEvaluationException> iter) {

        return new QueryObserver(metadata, iter);
    }

    protected class QueryObserver extends DelayedIteration<BindingSet, QueryEvaluationException> {

        private QueryLogRecord queryLogRecord;

        private Iteration<BindingSet, QueryEvaluationException> innerIter;

        private MeasuringIteration<BindingSet, QueryEvaluationException> measure;

        private MaterializationHandle handle;

        public QueryObserver(QueryLogRecord metadata, Iteration<BindingSet, QueryEvaluationException> iter) {
            queryLogRecord = metadata;
            innerIter = iter;
        }

        @Override
        protected Iteration<? extends BindingSet, ? extends QueryEvaluationException>
            createIteration() throws QueryEvaluationException {

            MaterializationManager manager = getFileManager();

            handle = manager.saveResult();

            Iteration<BindingSet, QueryEvaluationException> iter =
                    new QueryResultObservingIteration(handle, innerIter);

            measure = new MeasuringIteration<BindingSet,QueryEvaluationException>(iter);

            iter = measure;

            return iter;
        }

        @Override
        public BindingSet next() throws QueryEvaluationException {
            try {
                return super.next();
            } catch(QueryEvaluationException e) {
                handle.destroy();
                throw e;
            }
        }

        @Override
        public void handleClose() throws QueryEvaluationException {
            super.handleClose();

            queryLogRecord.setCardinality(measure.getCount());
            queryLogRecord.setDuration(measure.getStartTime(), measure.getEndTime());
            queryLogRecord.setResults(handle);

            try {
                qfrHandler.handleQueryRecord(queryLogRecord);
            } catch (QueryLogException e) {
                throw new QueryEvaluationException(e);
            }
        }

    }

}
