package eu.semagrow.stack.modules.sails.semagrow.rx;

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
public class OnSubscribeTupleResultsReactor implements Publisher {

    private static final Logger logger = LoggerFactory.getLogger(OnSubscribeTupleResultsReactor.class);

    private TupleQuery query;

    public OnSubscribeTupleResultsReactor(TupleQuery query) {
        this.query = query;
    }

    @Override
    public void subscribe(Subscriber subscriber) {
        subscriber.onSubscribe(new TupleQueryResultProducerReactor(subscriber, query));
    }

    public static final class TupleQueryResultProducerReactor implements Subscription, TupleQueryResultHandler {


        private Subscriber<? super BindingSet> subscriber;
        private TupleQuery query;

        public TupleQueryResultProducerReactor(Subscriber<? super BindingSet> o, TupleQuery query) {
            this.subscriber = o;
            this.query = query;

        }

        //////////////////////////

        @Override
        public void request(long l) {
            /*
            if (REQUESTED_UPDATER.get(this) == Long.MAX_VALUE) {
                // already started with fast-path
                return;
            }

            if (subscriber.isUnsubscribed())
                return;

            REQUESTED_UPDATER.set(this, Long.MAX_VALUE); */

            try {
                logger.debug("Sending query " + query.toString() + " with " + query.getBindings().toString());
                query.evaluate(this);
            } catch (Exception e) {
                subscriber.onError(e);
            }
        }

        @Override
        public void cancel() {

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
            logger.debug(bindingSet.toString());
            subscriber.onNext(bindingSet);
        }


    }
}
