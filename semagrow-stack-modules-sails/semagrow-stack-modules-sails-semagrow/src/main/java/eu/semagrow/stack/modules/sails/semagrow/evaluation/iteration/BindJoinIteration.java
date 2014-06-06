package eu.semagrow.stack.modules.sails.semagrow.evaluation.iteration;

import eu.semagrow.stack.modules.sails.semagrow.algebra.SourceQuery;
import info.aduna.iteration.CloseableIteration;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.evaluation.EvaluationStrategy;
import org.openrdf.query.algebra.evaluation.federation.JoinExecutorBase;

/**
 * Created by angel on 6/5/14.
 */
public class BindJoinIteration extends JoinExecutorBase<BindingSet> {

    private EvaluationStrategy evaluationStrategy;

    public BindJoinIteration(CloseableIteration<BindingSet,QueryEvaluationException> leftIter,
                             SourceQuery rightArg, org.openrdf.query.BindingSet bindings,
                             EvaluationStrategy strategy)
            throws org.openrdf.query.QueryEvaluationException {
        super(leftIter, rightArg, bindings);

        this.evaluationStrategy = strategy;
    }

    @Override
    protected void handleBindings() throws Exception {
        while (!closed && leftIter.hasNext()) {

        }
    }
}
