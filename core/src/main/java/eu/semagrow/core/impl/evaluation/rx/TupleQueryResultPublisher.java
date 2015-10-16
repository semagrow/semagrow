package eu.semagrow.core.impl.evaluation.rx;

import eu.semagrow.core.impl.evaluation.file.MaterializationManager;
import eu.semagrow.querylog.api.QueryLogHandler;
import org.openrdf.query.*;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.parser.ParsedTupleQuery;
import org.openrdf.query.parser.QueryParserUtil;
import org.openrdf.repository.sail.SailQuery;
import org.openrdf.repository.sail.SailTupleQuery;
import org.openrdf.repository.sparql.query.SPARQLTupleQuery;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by antonis on 9/4/2015.
 */
public class TupleQueryResultPublisher implements Publisher<BindingSet> {

    private static final Logger logger = LoggerFactory.getLogger(TupleQueryResultPublisher.class);

    private TupleQuery query;
    private String queryStr;
    private QueryLogHandler qfrHandler;
    private MaterializationManager mat;

    public TupleQueryResultPublisher(TupleQuery query, String queryStr, QueryLogHandler qfrHandler, MaterializationManager mat) {
        this.query = query;
        this.qfrHandler = qfrHandler;
        this.queryStr = queryStr;
        this.mat = mat;
    }

    public void subscribe(Subscriber<? super BindingSet> subscriber) {
        subscriber.onSubscribe(new TupleQueryResultProducer(subscriber, query, queryStr, qfrHandler, mat));
    }

    public static final class TupleQueryResultProducer implements Subscription {


        private Subscriber<? super BindingSet> subscriber;
        private TupleQuery query;
        private String queryStr;
        private boolean isEvaluating = false;
        private QueryLogHandler qfrHandler;
        private MaterializationManager mat;

        public TupleQueryResultProducer(Subscriber<? super BindingSet> o, TupleQuery query, String queryStr, QueryLogHandler qfrHandler, MaterializationManager mat) {
            this.subscriber = o;
            this.query = query;
            this.qfrHandler = qfrHandler;
            this.mat = mat;
            this.queryStr = queryStr;
        }

        //////////////////////////

        @Override
        public void request(long l) {

            //logger.debug("Requesting {} results and isEvaluating = {} ", l, isEvaluating);
            if (!isEvaluating) {
                try {
                    if (logger.isDebugEnabled())
                        logger.debug("Sending query {} with {}", query.toString().replace("\n", " "), query.getBindings());

                    isEvaluating = true;

                    TupleQueryResultHandler handler = new SubscribedQueryResultHandler(subscriber);

                    handler = new LoggingTupleQueryResultHandler(queryStr, handler, qfrHandler, mat);

                    query.evaluate(handler);

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
                subscriber.onNext(bindingSet);
            }
        }

    }
}
