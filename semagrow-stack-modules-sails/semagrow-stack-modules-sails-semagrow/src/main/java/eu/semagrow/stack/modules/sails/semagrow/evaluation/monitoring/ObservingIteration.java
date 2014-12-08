package eu.semagrow.stack.modules.sails.semagrow.evaluation.monitoring;

import info.aduna.iteration.Iteration;
import info.aduna.iteration.IterationWrapper;

/**
 * Created by angel on 6/12/14.
 */
public abstract class ObservingIteration<E,X extends Exception> extends IterationWrapper<E,X> {

    public ObservingIteration(Iteration<E,X> iter) {
        super(iter);
    }

    public abstract void observe(final E e) throws X;

    public abstract void observeExceptionally(final X x);

    @Override
    public E next() throws X {
        try {
            E item = super.next();
            observe(item);
            return item;
        } catch (Exception exception) {
            observeExceptionally((X)exception);
            throw (X)exception;
        }
    }
}
