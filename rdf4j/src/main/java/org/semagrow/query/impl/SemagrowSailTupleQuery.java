package org.semagrow.query.impl;

import org.semagrow.art.LogUtils;
import org.semagrow.query.SemagrowTupleQuery;
import org.semagrow.sail.SemagrowSailConnection;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.impl.IteratingTupleQueryResult;
import org.eclipse.rdf4j.query.parser.ParsedTupleQuery;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Title: SemagrowSailTupleQuery
 *
 * @author Angelos Charalambidis
 */

public class SemagrowSailTupleQuery extends SemagrowSailQuery implements SemagrowTupleQuery {

    private boolean includeProvenanceData = false;

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private String queryString;

    public SemagrowSailTupleQuery( ParsedTupleQuery query, String queryString, SailRepositoryConnection connection )
    {
        super(query, connection );
        this.queryString = queryString;
    }

    public TupleQueryResult evaluate() throws QueryEvaluationException {

        TupleExpr tupleExpr = getParsedQuery().getTupleExpr();

        try {
            CloseableIteration<? extends BindingSet, QueryEvaluationException> bindingsIter;

            SemagrowSailConnection sailCon = (SemagrowSailConnection) getConnection().getSailConnection();

            bindingsIter = sailCon.evaluate(tupleExpr, getActiveDataset(), getBindings(),
                    getIncludeInferred(), getIncludeProvenanceData(),
                    getIncludedSources(), getExcludedSources());

            bindingsIter = enforceMaxQueryTime(bindingsIter);

            return new IteratingTupleQueryResult(new ArrayList<String>(tupleExpr.getBindingNames()), bindingsIter);
        }
        catch (SailException e) {
            throw new QueryEvaluationException(e.getMessage(), e);
        }
    }

    public void evaluate(TupleQueryResultHandler handler)
            throws QueryEvaluationException, TupleQueryResultHandlerException
    {
        /*
        TupleQueryResult queryResult = evaluate();
        QueryResults.report(queryResult, handler);
        */

        logger.info("SemaGrow query evaluate with handler {}", this.queryString.replace('\n',' '));
        TupleExpr tupleExpr = getParsedQuery().getTupleExpr();

        try {
            Publisher<? extends BindingSet> result;

            SemagrowSailConnection sailCon = (SemagrowSailConnection) getConnection().getSailConnection();

            result = sailCon.evaluateReactive(tupleExpr, getActiveDataset(), getBindings(),
                    getIncludeInferred(), getIncludeProvenanceData(),
                    getIncludedSources(), getExcludedSources());

            //result = enforceMaxQueryTime(bindingsIter);

            logger.info("[{}] Query evaluation Start.", LogUtils.getQueryID());

            CountDownLatch latch = new CountDownLatch(1);

            HandlerSubscriberAdapter subscriberAdapter = toSubscriber(handler, latch);

            result.subscribe(subscriberAdapter);

            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.error("awaiting a latch", e);
            }

            if (subscriberAdapter.errorOccured) {
                if (subscriberAdapter.errorThrown instanceof QueryEvaluationException)
                    throw (QueryEvaluationException) subscriberAdapter.errorThrown;
                else
                    throw new QueryEvaluationException(subscriberAdapter.errorThrown);
            }

            logger.info("[{}] Query evaluation End.", LogUtils.getQueryID());
        }
        catch (SailException e) {
            throw new QueryEvaluationException(e.getMessage(), e);
        }
    }

    public void setIncludeProvenanceData(boolean includeProvenance) {
        includeProvenanceData = includeProvenance;
    }

    public boolean getIncludeProvenanceData() { return includeProvenanceData; }

    private HandlerSubscriberAdapter toSubscriber(TupleQueryResultHandler handler, CountDownLatch latch) {
        return new HandlerSubscriberAdapter(handler, latch);
    }


    private class HandlerSubscriberAdapter implements Subscriber<BindingSet>
    {
        private TupleQueryResultHandler handler;

        private boolean isStarted = false;

        private boolean errorOccured = false;

        private Throwable errorThrown = null;

        private CountDownLatch latch;

        private Subscription subscription;

        private int resultsCount = 0;

        public HandlerSubscriberAdapter(TupleQueryResultHandler handler, CountDownLatch latch) {
            this.handler = handler;
            this.latch = latch;
        }

        public void onSubscribe(Subscription subscription) {
            // FIXME: is it possible that tuplequeryhandler be slower than producer?
            this.subscription = subscription;
            subscription.request(Long.MAX_VALUE);
        }

        public void onNext(BindingSet bindings) {
            try {
                if (!isStarted) {
                    synchronized (handler) {
                        handler.startQueryResult(new ArrayList<>(bindings.getBindingNames()));
                    }
                    isStarted = true;
                    logger.info("Found first result.");
                }
                synchronized (handler) {
                    handler.handleSolution(bindings);
                }
                logger.info("-> Found " + bindings);
                resultsCount++;
            } catch (TupleQueryResultHandlerException e) {
                subscription.cancel();
                logger.error("Tuple handle solution error", e);
            }
        }


        public void onError(Throwable throwable) {
            errorOccured = true;
            errorThrown = throwable;

            logger.error("Evaluation error", throwable);

            if (isStarted) {
                try {
                    synchronized (handler) {
                        handler.endQueryResult();
                    }
                } catch (TupleQueryResultHandlerException e) {
                    logger.error("Tuple handle solution error", e);
                }
            } else {
                try {
                    synchronized (handler) {
                        handler.startQueryResult(Collections.emptyList());
                        handler.endQueryResult();
                    }
                } catch (TupleQueryResultHandlerException e) {
                    logger.error("Tuple handle solution error", e);
                }
            }
            //subscription.cancel();
            latch.countDown();
        }


        public void onComplete() {
            logger.info("Found " + resultsCount + " results.");
            if (isStarted) {
                try {
                    synchronized (handler) {
                        handler.endQueryResult();
                    }
                } catch (TupleQueryResultHandlerException e) {
                    logger.error("Tuple handle solution error", e);
                }
            } else {
                try {
                    synchronized (handler) {
                        handler.startQueryResult(Collections.emptyList());
                        handler.endQueryResult();
                    }
                } catch (TupleQueryResultHandlerException e) {
                    logger.error("Tuple handle solution error", e);
                }
            }
            //subscription.cancel();
            latch.countDown();
        }
    }
}
