package eu.semagrow.stack.modules.sails.semagrow.evaluation.iteration;

import info.aduna.iteration.Iteration;
import info.aduna.iteration.IterationWrapper;

/**
 * Created by angel on 6/12/14.
 */
public abstract class RecordingIteration<E,X extends Exception> extends IterationWrapper<E,X> {

    public RecordingIteration(Iteration<E,X> iter) {
        super(iter);
    }

    public abstract void record(E e);

    public abstract void record(X x);

    @Override
    public E next() throws X {
        try {
            E item = super.next();
            record(item);
            return item;
        } catch (Exception exception) {
            record((X)exception);
            throw (X)exception;
        }
    }
}
