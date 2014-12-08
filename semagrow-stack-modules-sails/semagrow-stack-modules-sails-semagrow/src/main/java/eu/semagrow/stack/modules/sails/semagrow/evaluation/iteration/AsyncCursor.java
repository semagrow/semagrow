package eu.semagrow.stack.modules.sails.semagrow.evaluation.iteration;

import info.aduna.iteration.*;
import org.openrdf.query.QueryEvaluationException;

import java.util.concurrent.*;

/**
 * Created by angel on 6/5/14.
 */
public abstract class AsyncCursor<E,X extends Exception> extends CloseableIterationBase<E,X> {

    protected Future<Iteration<E, X>> future;

    private Iteration<? extends E, ? extends X> iter;

    //public AsyncCursor(Future<CloseableIteration<E, QueryEvaluationException>> future) {
    //    this.future = future;
    //}

    public AsyncCursor(ExecutorService executorService) {

        future = executorService.submit(new Callable<Iteration<E, X>>() {
            @Override
            public Iteration<E,X> call() throws Exception {
                return createIteration();
            }
        });

    }

    protected abstract Iteration<E, X>
        createIteration() throws X;

    @Override
    protected void handleClose() throws X {
        if (iter != null)
            Iterations.closeCloseable(iter);
        else
            future.cancel(true);
    }

    public boolean hasNext() throws X {
        try {
            if (iter == null)
                iter = future.get();

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        if (iter != null)
            return iter.hasNext();

        return false;
    }

    public E next() throws X {
        try {
            if (iter == null)
                iter = future.get();

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        if (iter != null && iter.hasNext())
            return iter.next();
        else
            return null;
    }

    public void remove() throws X {

        if (iter == null || isClosed()) {
            throw new IllegalStateException();
        }

        iter.remove();
    }
}
