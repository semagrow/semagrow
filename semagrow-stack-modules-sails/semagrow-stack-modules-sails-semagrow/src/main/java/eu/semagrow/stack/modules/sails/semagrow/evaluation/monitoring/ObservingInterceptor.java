package eu.semagrow.stack.modules.sails.semagrow.evaluation.monitoring;

import eu.semagrow.stack.modules.sails.semagrow.evaluation.interceptors.AbstractEvaluationSessionAwareInterceptor;
import eu.semagrow.stack.modules.sails.semagrow.evaluation.interceptors.QueryExecutionInterceptor;
import eu.semagrow.stack.modules.sails.semagrow.evaluation.monitoring.logging.LoggerWithQueue;
import eu.semagrow.stack.modules.sails.semagrow.evaluation.monitoring.querylog.QueryLogRecord;
import eu.semagrow.stack.modules.sails.semagrow.evaluation.monitoring.querylog.impl.QueryLogRecordImpl;
import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.Iteration;
import info.aduna.iteration.Iterations;

import org.openrdf.model.URI;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by angel on 6/30/14.
 * @author Angelos Charalampidis
 * @author Giannis Mouchakis
 */
@Deprecated
public class ObservingInterceptor
        extends AbstractEvaluationSessionAwareInterceptor
        implements QueryExecutionInterceptor {
	
	BlockingQueue<Object> queue = new ArrayBlockingQueue<Object>(1000000);
    LoggerWithQueue logWritter = new LoggerWithQueue(queue);
    Thread logWritterThread = new Thread(logWritter);
    
    final Logger logger = LoggerFactory.getLogger(ObservingInterceptor.class);

    public CloseableIteration<BindingSet, QueryEvaluationException>
        afterExecution(URI endpoint, TupleExpr expr, BindingSet bindings, CloseableIteration<BindingSet, QueryEvaluationException> result) {

        QueryLogRecordImpl metadata = createMetadata(endpoint, expr, bindings.getBindingNames());
        return observe(metadata, result);
    }

    public CloseableIteration<BindingSet, QueryEvaluationException>
        afterExecution(URI endpoint, TupleExpr expr, CloseableIteration<BindingSet, QueryEvaluationException> bindingIter, CloseableIteration<BindingSet, QueryEvaluationException> result) {

        List<BindingSet> bindings = Collections.<BindingSet>emptyList();

        try {
            bindings = Iterations.asList(bindingIter);
        } catch (Exception e) {

        }

//        bindingIter = new CollectionIteration<BindingSet, QueryEvaluationException>(bindings);

        Set<String> bindingNames = (bindings.size() == 0) ? new HashSet<String>() : bindings.get(0).getBindingNames();

        QueryLogRecordImpl metadata = createMetadata(endpoint, expr, bindingNames);

        return observe(metadata, result);
    }


    public CloseableIteration<BindingSet, QueryEvaluationException>
        observe(QueryLogRecordImpl metadata, CloseableIteration<BindingSet, QueryEvaluationException> iter) {
    	if ( ! logWritterThread.isAlive()) {
        	logWritterThread.start();
        }
        return new QueryObserver(metadata, iter);
    }


    protected QueryLogRecordImpl createMetadata(URI endpoint, TupleExpr expr, Set<String> bindingNames) {
        return new QueryLogRecordImpl(this.getQueryEvaluationSession(), endpoint, expr, bindingNames);
    }

    protected class QueryObserver extends ObservingIteration<BindingSet,QueryEvaluationException> {

        private QueryLogRecord metadata;
        
        public QueryObserver(QueryLogRecord metadata, Iteration<BindingSet, QueryEvaluationException> iter) {
            super(iter);
            this.metadata = metadata;
            try {
				queue.put(metadata.getSession().getSessionId());
				queue.put(Long.toString(System.currentTimeMillis()));
				queue.put(metadata.getEndpoint());
				queue.put("@");
				queue.put(metadata.getQuery());
				queue.put("@");
				queue.put(metadata.getBindingNames());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        }

		@Override
		public void observe(BindingSet bindings) throws QueryEvaluationException {
			try {
				queue.put(bindings.getBindingNames().size());
				queue.put(bindings);
				for (String name : metadata.getBindingNames()) {
					queue.put(bindings.getValue(name).stringValue());
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}

		@Override
		public void observeExceptionally(QueryEvaluationException x) {
			//logWritter.finish();//TODO:log this? 
			
		}
		
        /*
        @Override
        public void handleClose() throws QueryEvaluationException {
        	
        }
        */
		
    }

}
