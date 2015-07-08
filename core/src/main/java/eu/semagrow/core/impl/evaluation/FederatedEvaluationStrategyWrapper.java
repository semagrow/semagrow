package eu.semagrow.core.impl.evaluation;

import eu.semagrow.core.evaluation.FederatedEvaluationStrategy;
import eu.semagrow.core.evaluation.QueryExecutor;
import info.aduna.iteration.CloseableIteration;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;

/**
 * Created by angel on 6/12/14.
 */
public class FederatedEvaluationStrategyWrapper implements FederatedEvaluationStrategy {

    private FederatedEvaluationStrategy wrappedStrategy;

    public FederatedEvaluationStrategyWrapper(FederatedEvaluationStrategy wrapped) {
        assert wrapped != null;
        wrappedStrategy = wrapped;
    }

    protected FederatedEvaluationStrategy getWrappedStrategy() {
        return wrappedStrategy;
    }

    public CloseableIteration<BindingSet, QueryEvaluationException>
        evaluate(TupleExpr tupleExpr, CloseableIteration<BindingSet, QueryEvaluationException> bIter)
            throws QueryEvaluationException {
        return getWrappedStrategy().evaluate(tupleExpr, bIter);
    }

    public CloseableIteration<BindingSet, QueryEvaluationException>
        evaluate(TupleExpr tupleExpr, BindingSet bindings)
            throws QueryEvaluationException {
        return getWrappedStrategy().evaluate(tupleExpr, bindings);
    }

    public Value evaluate(ValueExpr valueExpr, BindingSet bindings)
            throws ValueExprEvaluationException, QueryEvaluationException {
        return getWrappedStrategy().evaluate(valueExpr, bindings);
    }

    public boolean isTrue(ValueExpr valueExpr, BindingSet bindings)
            throws ValueExprEvaluationException, QueryEvaluationException {
        return getWrappedStrategy().isTrue(valueExpr, bindings);
    }

    public void setIncludeProvenance(boolean p) { getWrappedStrategy().setIncludeProvenance(p); }

    public QueryExecutor getQueryExecutor() { return getWrappedStrategy().getQueryExecutor(); }

    public void setQueryExecutor(QueryExecutor executor) { getWrappedStrategy().setQueryExecutor(executor); }

}
