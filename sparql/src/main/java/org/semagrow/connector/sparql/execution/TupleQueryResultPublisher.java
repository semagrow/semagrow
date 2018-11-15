package org.semagrow.connector.sparql.execution;

import org.semagrow.evaluation.LoggingTupleQueryResultHandler;
import org.semagrow.evaluation.file.MaterializationManager;
import org.semagrow.querylog.api.QueryLogHandler;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.*;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by antonis on 9/4/2015.
 */
public class TupleQueryResultPublisher implements Publisher<BindingSet> {

    private static final Logger logger = LoggerFactory.getLogger(TupleQueryResultPublisher.class);

    private TupleQuery query;
    private String queryStr;
    private QueryLogHandler qfrHandler;
    private MaterializationManager mat;
    private URL endpoint;

    private static ExecutorService executorService = Executors.newCachedThreadPool();

    public TupleQueryResultPublisher(TupleQuery query, String queryStr, QueryLogHandler qfrHandler, MaterializationManager mat, URL endpoint) {
        this.query = query;
        this.qfrHandler = qfrHandler;
        this.queryStr = queryStr;
        this.mat = mat;
        this.endpoint = endpoint;
    }

    public void subscribe(Subscriber<? super BindingSet> subscriber) {
        subscriber.onSubscribe(new TupleQuerySubscription(subscriber, query, queryStr, qfrHandler, mat, endpoint));
    }

    static class TupleQueryProducer implements Runnable {

        private TupleQuery query;
        private Subscriber<? super BindingSet> subscriber;

        private long requested = 0;
        private Boolean shutdownFlag = false;
        private CountDownLatch latch = new CountDownLatch(0);

        final private Object syncRequest = new Object();

        public TupleQueryProducer(Subscriber<? super BindingSet> o, TupleQuery query, String queryStr, QueryLogHandler qfrHandler, MaterializationManager mat, URL endpoint) {
            this.query = query;
            this.subscriber = o;
        }

        public void requestMore(long n) {
            latch.countDown();
            synchronized (syncRequest) {
                requested += n;
            }
        }

        public void fullfillOne() {
            synchronized (syncRequest) {
                assert requested > 0;
                requested--;
            }
        }

        public void awaitForRequests() throws InterruptedException {
            synchronized (syncRequest) {
                if (requested == 0)
                    latch.await();
            }
        }

        public void run() {

            TupleQueryResult result = null;

            try {
                awaitForRequests();
                result = query.evaluate();

                while(result.hasNext()) {
                    awaitForRequests();
                    BindingSet b = result.next();
                    logger.debug("found " + b);
                    fullfillOne();
                    subscriber.onNext(b);
                }

                if (!result.hasNext()) {
                    subscriber.onComplete();
                }

            } catch (QueryEvaluationException e) {
                logger.warn("Error while evaluating subquery", e);
                subscriber.onError(e);

            } catch (InterruptedException i) {
                if (shutdownFlag)
                    logger.info("Subscription shutdown by subscriber. Interrupted.");
                else
                    logger.warn("SubQuery thread interrupted.");
            } finally {
                if (result != null)
                    result.close();
            }
        }

        public void shutdown() {
            shutdownFlag = true;
        }

    }

    public static final class TupleQuerySubscription implements Subscription {


        private Subscriber<? super BindingSet> subscriber;
        private TupleQuery query;
        private String queryStr;
        private QueryLogHandler qfrHandler;
        private MaterializationManager mat;
        private URL endpoint;
        private Future<?> f;

        private TupleQueryProducer producer;

        public TupleQuerySubscription(Subscriber<? super BindingSet> o, TupleQuery query, String queryStr, QueryLogHandler qfrHandler, MaterializationManager mat, URL endpoint) {
            this.subscriber = o;
            this.query = query;
            this.qfrHandler = qfrHandler;
            this.mat = mat;
            this.queryStr = queryStr;
            this.endpoint = endpoint;
            logger.debug("new TupleQueryResultProducer - {}", endpoint);
        }

        //////////////////////////

        @Override
        public void request(long l) {

            if (producer == null) {

                if (logger.isDebugEnabled())
                    logger.debug("Sending query {} with {}", query.toString().replace("\n", " "), query.getBindings());

                producer = new TupleQueryProducer(subscriber, query, queryStr, qfrHandler, mat, endpoint);
                //executorService.execute(producer);
                Future<?> f = executorService.submit(producer);
                producer.requestMore(l);

            } else {
                producer.requestMore(l);
            }
        }

        @Override
        public void cancel() {

            if (producer != null) {
                producer.shutdown();
                assert f != null;
                if (!f.isDone() && !f.isCancelled())
                    f.cancel(true);
            }
        }
    }
}
