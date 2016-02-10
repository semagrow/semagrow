package eu.semagrow.core.impl.evaluation.iteration;

import eu.semagrow.core.evaluation.EvaluationStrategy;
import info.aduna.iteration.CloseableIteration;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.federation.JoinExecutorBase;

/**
 * Created by angel on 6/5/14.
 */
public class BindJoinIteration extends JoinExecutorBase<BindingSet> {

    private EvaluationStrategy evaluationStrategy;

    public BindJoinIteration(CloseableIteration<BindingSet,QueryEvaluationException> leftIter,
                             TupleExpr rightArg, org.openrdf.query.BindingSet bindings,
                             EvaluationStrategy strategy)
            throws org.openrdf.query.QueryEvaluationException {
        super(leftIter, rightArg, bindings);

        this.evaluationStrategy = strategy;
        run();
    }

    @Override
    protected void handleBindings() throws Exception {
        addResult(evaluationStrategy.evaluate(rightArg,
                new BNodeFilteringIteration<QueryEvaluationException>(rightArg.getBindingNames(),leftIter)));
    }
}
