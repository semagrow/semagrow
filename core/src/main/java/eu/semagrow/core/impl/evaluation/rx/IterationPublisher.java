package eu.semagrow.core.impl.evaluation.rx;

import info.aduna.iteration.Iteration;
import info.aduna.iteration.Iterations;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Created by angel on 28/3/2015.
 */
public class IterationPublisher<T> implements Publisher<T> {

    private final Iteration<T, ? extends Exception> iter;

    public IterationPublisher(Iteration<T, ? extends Exception> iter){
        this.iter = iter;
    }

    @Override
    public void subscribe(Subscriber<? super T> subscriber) {

        subscriber.onSubscribe(new Subscription() {

            boolean canceled = false;
            @Override
            public void request(long l) {

                if (canceled)
                    return;

                if (l ==  Long.MAX_VALUE) {
                    try {
                        while (iter.hasNext()) {
                            subscriber.onNext(iter.next());
                        }
                        subscriber.onComplete();
                        Iterations.closeCloseable(iter);
                    } catch (Exception e) {
                        subscriber.onError(e);
                    }
                } else if (l > 0) {

                    long numEmit = l;

                    try {
                        while (iter.hasNext() && --numEmit >= 0) {
                            subscriber.onNext(iter.next());
                        }

                        if (!iter.hasNext()) {
                            subscriber.onComplete();
                            Iterations.closeCloseable(iter);
                        }
                    } catch (Exception e) {
                        subscriber.onError(e);
                    }
                }
            }

            @Override
            public void cancel() {
                canceled = true;
            }
        });
    }
}
