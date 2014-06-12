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

    @Override
    public E next() throws X {
        E item = super.next();
        record(item);
        return item;
    }
}
