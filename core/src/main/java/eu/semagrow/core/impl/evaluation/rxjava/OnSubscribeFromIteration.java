package eu.semagrow.core.impl.evaluation.rxjava;

import org.eclipse.rdf4j.common.iteration.Iteration;
import org.eclipse.rdf4j.common.iteration.Iterations;
import rx.Observable;
import rx.Producer;
import rx.Subscriber;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * Created by angel on 11/25/14.
 */
public class OnSubscribeFromIteration<T> implements Observable.OnSubscribe<T> {

    final Iteration<? extends T, ? extends Exception> it;

    public OnSubscribeFromIteration(Iteration<? extends T, ? extends Exception> it) {
        this.it = it;
    }

    @Override
    public void call(final Subscriber<? super T> o) {
        o.setProducer(new IterationProducer<T>(o, it));

    }

    private static final class IterationProducer<T> implements Producer {
        private final Subscriber<? super T> o;
        private final Iteration<? extends T, ? extends Exception> it;

        private volatile long requested = 0;

        @SuppressWarnings("rawtypes")
        private static final AtomicLongFieldUpdater<IterationProducer> REQUESTED_UPDATER = AtomicLongFieldUpdater.newUpdater(IterationProducer.class, "requested");

        private IterationProducer(Subscriber<? super T> o, Iteration<? extends T, ? extends Exception> it) {
            this.o = o;
            this.it = it;
        }

        @Override
        public void request(long n) {
            if (REQUESTED_UPDATER.get(this) == Long.MAX_VALUE) {
                // already started with fast-path
                return;
            }
            if (n == Long.MAX_VALUE) {
                REQUESTED_UPDATER.set(this, n);
                // fast-path without backpressure
                try {
                    while (it.hasNext()) {
                        if (o.isUnsubscribed()) {
                            return;
                        }
                        o.onNext(it.next());
                    }
                } catch (Exception e) {
                    o.onError(e);
                }
                if (!o.isUnsubscribed()) {
                    o.onCompleted();
                    try {
                        Iterations.closeCloseable(it);
                    } catch (Exception e) { }
                }
            } else if (n > 0) {
                // backpressure is requested
                long _c = REQUESTED_UPDATER.getAndAdd(this, n);
                if (_c == 0) {
                    while (true) {
                        /*
                         * This complicated logic is done to avoid touching the volatile `requested` value
                         * during the loop itself. If it is touched during the loop the performance is impacted significantly.
                         */
                        long r = requested;
                        long numToEmit = r;
                        try {
                            while (it.hasNext() && --numToEmit >= 0) {
                                if (o.isUnsubscribed()) {
                                    return;
                                }
                                o.onNext(it.next());
                            }
                        } catch (Exception e) {
                            o.onError(e);
                        }

                        try {
                            if (!it.hasNext()) {
                                o.onCompleted();
                                try {
                                    Iterations.closeCloseable(it);
                                } catch (Exception e) { }
                                return;
                            }
                        } catch (Exception e) {
                            o.onError(e);
                        }

                        if (REQUESTED_UPDATER.addAndGet(this, -r) == 0) {
                            // we're done emitting the number requested so return
                            return;
                        }

                    }
                }
            }

        }

    }
}