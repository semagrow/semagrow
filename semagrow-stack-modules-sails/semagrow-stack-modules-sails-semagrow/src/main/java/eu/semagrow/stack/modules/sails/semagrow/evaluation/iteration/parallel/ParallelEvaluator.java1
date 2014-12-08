package eu.semagrow.stack.modules.sails.semagrow.evaluation.iteration.parallel;

import info.aduna.iteration.CloseableIteration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;

/**
 *
 * @author http://www.turnguard.com/turnguard
 */
public interface ParallelEvaluator {
    public CompletableFuture<CloseableIteration<BindingSet,QueryEvaluationException>> getLeftArgCompletableFuture();
    public CompletableFuture<CloseableIteration<BindingSet,QueryEvaluationException>> getRightArgCompletableFuture();
    public CloseableIteration<BindingSet,QueryEvaluationException> getLeftArg() throws InterruptedException, ExecutionException;
    public CloseableIteration<BindingSet,QueryEvaluationException> getRightArg() throws InterruptedException, ExecutionException;
}
