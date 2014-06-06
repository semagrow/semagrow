package eu.semagrow.stack.modules.sails.semagrow.evaluation.iteration;

import eu.semagrow.stack.modules.sails.semagrow.algebra.SourceQuery;
import info.aduna.iteration.CloseableIteration;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.evaluation.EvaluationStrategy;
import org.openrdf.query.algebra.evaluation.federation.JoinExecutorBase;
import org.openrdf.query.algebra.evaluation.iterator.CollectionIteration;

import java.util.ArrayList;

/**
 * Created by angel on 6/5/14.
 */
public class BatchingBindJoinIteration extends JoinExecutorBase<BindingSet> {

    private EvaluationStrategy evaluationStrategy;

    private final int blockSize;

    public BatchingBindJoinIteration(CloseableIteration<BindingSet,QueryEvaluationException> leftIter,
                             SourceQuery rightArg, org.openrdf.query.BindingSet bindings,
                             EvaluationStrategy strategy)
            throws org.openrdf.query.QueryEvaluationException {
        super(leftIter, rightArg, bindings);

        this.blockSize = 10;
        this.evaluationStrategy = strategy;
    }

    @Override
    protected void handleBindings() throws Exception {
        while (!closed && leftIter.hasNext()) {

            ArrayList<BindingSet> blockBindings = new ArrayList<BindingSet>(blockSize);
            for (int i = 0; i < blockSize; i++) {
                if (!leftIter.hasNext())
                    break;
                blockBindings.add(leftIter.next());
            }
            CloseableIteration<BindingSet, QueryEvaluationException> materializedIter =
                    new CollectionIteration<BindingSet, QueryEvaluationException>(blockBindings);
            //addResult(evaluateInternal(service, materializedIter, service.getBaseURI()));
        }
    }

}
