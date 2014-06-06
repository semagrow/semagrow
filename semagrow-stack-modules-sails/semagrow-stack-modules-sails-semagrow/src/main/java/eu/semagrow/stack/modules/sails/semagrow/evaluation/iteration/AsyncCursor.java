package eu.semagrow.stack.modules.sails.semagrow.evaluation.iteration;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.LookAheadIteration;
import org.openrdf.query.QueryEvaluationException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by angel on 6/5/14.
 */
public class AsyncCursor<E> extends LookAheadIteration<E,QueryEvaluationException> {

    protected Future<CloseableIteration<E, QueryEvaluationException>> future;
    protected CloseableIteration<E, QueryEvaluationException> result;

    public AsyncCursor(Future<CloseableIteration<E, QueryEvaluationException>> future) {
        this.future = future;
    }

    @Override
    protected void handleClose() throws QueryEvaluationException {
        if (result != null)
            result.close();
        else
            future.cancel(true);
    }

    @Override
    protected E getNextElement() throws QueryEvaluationException {
        try {
            if (result == null)
                result = future.get();

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        if (result != null && result.hasNext())
            return result.next();
        else
            return null;
    }
}
