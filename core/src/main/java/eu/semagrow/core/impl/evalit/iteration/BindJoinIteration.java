package eu.semagrow.core.impl.evalit.iteration;

import eu.semagrow.core.evalit.EvaluationStrategy;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.JoinExecutorBase;

/**
 * Created by angel on 6/5/14.
 */
public class BindJoinIteration extends JoinExecutorBase<BindingSet> {

    private EvaluationStrategy evaluationStrategy;

    public BindJoinIteration(CloseableIteration<BindingSet,QueryEvaluationException> leftIter,
                             TupleExpr rightArg, org.eclipse.rdf4j.query.BindingSet bindings,
                             EvaluationStrategy strategy)
            throws org.eclipse.rdf4j.query.QueryEvaluationException {
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
