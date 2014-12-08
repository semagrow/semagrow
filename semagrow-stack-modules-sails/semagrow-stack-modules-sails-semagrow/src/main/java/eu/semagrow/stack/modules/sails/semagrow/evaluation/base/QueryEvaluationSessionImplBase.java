package eu.semagrow.stack.modules.sails.semagrow.evaluation.base;

import eu.semagrow.stack.modules.api.evaluation.*;
import eu.semagrow.stack.modules.api.evaluation.EvaluationStrategy;
import eu.semagrow.stack.modules.sails.semagrow.evaluation.SessionUUID;
import eu.semagrow.stack.modules.sails.semagrow.evaluation.interceptors.AbstractEvaluationSessionAwareInterceptor;
import eu.semagrow.stack.modules.sails.semagrow.evaluation.interceptors.InterceptingEvaluationStrategy;
import eu.semagrow.stack.modules.sails.semagrow.evaluation.interceptors.QueryEvaluationInterceptor;
import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.IterationWrapper;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.*;

import javax.management.QueryEval;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by angel on 6/12/14.
 */
public abstract class QueryEvaluationSessionImplBase
        implements QueryEvaluationSession {

    private SessionUUID id;

    public QueryEvaluationSessionImplBase() {
        this.id = SessionUUID.createUniqueId();
    }

    public SessionId getSessionId() { return id; }

    public EvaluationStrategy getEvaluationStrategy() {
        EvaluationStrategy actualStrategy = getEvaluationStrategyInternal();
        attachStrategyInterceptors(actualStrategy);
        return actualStrategy;
    }

    protected abstract EvaluationStrategy getEvaluationStrategyInternal();

    protected void attachStrategyInterceptors(EvaluationStrategy strategy) {
       if (strategy instanceof InterceptingEvaluationStrategy)
           attachStrategyInterceptors((InterceptingEvaluationStrategy)strategy);
    }

    protected void attachStrategyInterceptors(InterceptingEvaluationStrategy strategy) {
        Collection<QueryEvaluationInterceptor> interceptors =  getStrategyInterceptors();
        for (QueryEvaluationInterceptor interceptor : interceptors) {
            interceptor.setQueryEvaluationSession(this);
            strategy.addEvaluationInterceptor(interceptor);
        }
    }

    protected Collection<QueryEvaluationInterceptor> getStrategyInterceptors() {
        List<QueryEvaluationInterceptor> interceptors = new LinkedList<QueryEvaluationInterceptor>();
        interceptors.add(new SessionAwareInterceptor());
        return interceptors;
    }

    public void initializeSession() { }

    public void closeSession() { }

    protected class SessionAwareInterceptor
            extends AbstractEvaluationSessionAwareInterceptor
            implements QueryEvaluationInterceptor {

        public CloseableIteration<BindingSet, QueryEvaluationException>
            afterEvaluation(TupleExpr expr, BindingSet bindings, CloseableIteration<BindingSet, QueryEvaluationException> result) {
            return afterEvaluation(expr,result);
        }

        public CloseableIteration<BindingSet, QueryEvaluationException>
            afterEvaluation(TupleExpr expr, CloseableIteration<BindingSet, QueryEvaluationException> bindings, CloseableIteration<BindingSet, QueryEvaluationException> result) {
            return afterEvaluation(expr,result);
        }

        protected CloseableIteration<BindingSet, QueryEvaluationException>
            afterEvaluation(TupleExpr expr, CloseableIteration<BindingSet, QueryEvaluationException> result) {

            if (isRoot(expr))
                result = new SessionAwareIteration(result);

            return result;
        }

        private boolean isRoot(TupleExpr expr) { return expr.getParentNode() == null; }
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
