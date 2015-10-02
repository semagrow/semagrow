package eu.semagrow.core.impl.evaluation.rx;

import org.openrdf.query.*;
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

    public TupleQueryResultPublisher(TupleQuery query) {
        this.query = query;
    }

    public void subscribe(Subscriber<? super BindingSet> subscriber) {
        subscriber.onSubscribe(new TupleQueryResultProducer(subscriber, query));
    }

    public static final class TupleQueryResultProducer implements Subscription, TupleQueryResultHandler {


        private Subscriber<? super BindingSet> subscriber;
        private TupleQuery query;
        private boolean isEvaluating = false;

        public TupleQueryResultProducer(Subscriber<? super BindingSet> o, TupleQuery query) {
            this.subscriber = o;
            this.query = query;

        }

        //////////////////////////

        @Override
        public void request(long l) {

            //logger.debug("Requesting {} results and isEvaluating = {} ", l, isEvaluating);
            if (!isEvaluating) {
                try {
                    if (logger.isDebugEnabled())
                        logger.debug("Query {} with {}", query.toString().replace("\n", " "), query.getBindings());

                    isEvaluating = true;
                    query.evaluate(this);
                } catch (Exception e) {
                    subscriber.onError(e);
                }
            }
        }

        @Override
        public void cancel() {
            isEvaluating = false;
        }

        //////////////////////////

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
