package eu.semagrow.stack.modules.sails.semagrow.evaluation;

import eu.semagrow.stack.modules.api.evaluation.EvaluationStrategy;
import eu.semagrow.stack.modules.api.evaluation.QueryEvaluationSession;
import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.IterationWrapper;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;

/**
 * Created by angel on 6/12/14.
 */
public abstract class QueryEvaluationSessionImplBase implements QueryEvaluationSession {

    public EvaluationStrategy getEvaluationStrategy() {
        EvaluationStrategy actualStrategy = getEvaluationStrategyInternal();
        return new SessionAwareEvaluationStrategy(actualStrategy);
    }

    protected abstract EvaluationStrategy getEvaluationStrategyInternal();

    public void initializeSession() { }

    public void closeSession() { }

    protected class SessionAwareEvaluationStrategy extends EvaluationStrategyWrapper {

        public SessionAwareEvaluationStrategy(EvaluationStrategy evaluationStrategy) {

            super(evaluationStrategy);
        }

        @Override
        public CloseableIteration<BindingSet,QueryEvaluationException>
            evaluate(TupleExpr expr, BindingSet bindings) throws QueryEvaluationException {

            initializeSession();
            return new SessionAwareIteration(super.evaluate(expr,bindings));
        }

        protected class SessionAwareIteration extends IterationWrapper<BindingSet,QueryEvaluationException> {

            public SessionAwareIteration(CloseableIteration<BindingSet,QueryEvaluationException> iter) {
                super(iter);
            }

            @Override
            public void handleClose() throws QueryEvaluationException {
                super.handleClose();
                closeSession();
            }
        }
    }

}
