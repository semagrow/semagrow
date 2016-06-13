package eu.semagrow.core.impl.evaluation;

import eu.semagrow.core.impl.evaluation.file.MaterializationManager;
import eu.semagrow.querylog.api.QueryLogHandler;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.*;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by antonis on 9/4/2015.
 */
public class TupleQueryResultPublisher implements Publisher<BindingSet> {

    private static final Logger logger = LoggerFactory.getLogger(TupleQueryResultPublisher.class);

    private TupleQuery query;
    private String queryStr;
    private QueryLogHandler qfrHandler;
    private MaterializationManager mat;
    private IRI endpoint;

    private static ExecutorService executorService = Executors.newCachedThreadPool();

    public TupleQueryResultPublisher(TupleQuery query, String queryStr, QueryLogHandler qfrHandler, MaterializationManager mat, IRI endpoint) {
        this.query = query;
        this.qfrHandler = qfrHandler;
        this.queryStr = queryStr;
        this.mat = mat;
        this.endpoint = endpoint;
    }

    public void subscribe(Subscriber<? super BindingSet> subscriber) {
        subscriber.onSubscribe(new TupleQueryResultProducer(subscriber, query, queryStr, qfrHandler, mat, endpoint));
    }

    public static final class TupleQueryResultProducer implements Subscription {


        private Subscriber<? super BindingSet> subscriber;
        private TupleQuery query;
        private String queryStr;
        private boolean isEvaluating = false;
        private QueryLogHandler qfrHandler;
        private MaterializationManager mat;
        private IRI endpoint;

        public TupleQueryResultProducer(Subscriber<? super BindingSet> o, TupleQuery query, String queryStr, QueryLogHandler qfrHandler, MaterializationManager mat, IRI endpoint) {
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

            logger.debug("Requesting {} results and isEvaluating = {} ", l, isEvaluating);
            if (!isEvaluating) {
                try {
                    if (logger.isDebugEnabled())
                        logger.debug("Sending query {} with {}", query.toString().replace("\n", " "), query.getBindings());

                    isEvaluating = true;

                    final Map<String,String> contextMap = MDC.getCopyOfContextMap();

                    Runnable task = new Runnable() {
                        @Override
                        public void run() {
                            MDC.setContextMap(contextMap);
                            TupleQueryResultHandler handler = new SubscribedQueryResultHandler(subscriber);

                            //handler = new LoggingTupleQueryResultHandler(queryStr, handler, qfrHandler, mat, endpoint);
                            handler = new LoggingTupleQueryResultHandler(queryStr, handler, null, mat, endpoint);
                            try {
                                query.evaluate(handler);
                            } catch (Exception e) {
                            subscriber.onError(e);
                            }
                        }
                    };

                    executorService.execute(task);

                } catch (Exception e) {
                    subscriber.onError(e);
                }
            }
        }

        @Override
        public void cancel() {
            isEvaluating = false;
        }


        private class SubscribedQueryResultHandler implements TupleQueryResultHandler
        {

            private final Subscriber<? super BindingSet> subscriber;

            public SubscribedQueryResultHandler(Subscriber<? super BindingSet> subscriber) {
                this.subscriber = subscriber;
            }

            @Override
            public void handleBoolean(boolean b) throws QueryResultHandlerException {

            }

            @Override
            public void handleLinks(List<String> list) throws QueryResultHandlerException {

            }

            @Override
            public void startQueryResult(List<String> list) throws TupleQueryResultHandlerException {
            }

            @Override
            public void endQueryResult() throws TupleQueryResultHandlerException {
                subscriber.onComplete();
            }

            @Override
            public void handleSolution(BindingSet bindingSet) throws TupleQueryResultHandlerException {
                logger.debug("found " + bindingSet);
                subscriber.onNext(bindingSet);
            }
        }

    }
}
