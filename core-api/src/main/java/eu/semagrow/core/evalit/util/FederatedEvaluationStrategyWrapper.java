package eu.semagrow.core.evalit.util;

import eu.semagrow.core.evalit.FederatedEvaluationStrategy;
import eu.semagrow.core.evalit.QueryExecutor;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;

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
    evaluate(Service tupleExpr, String s, CloseableIteration<BindingSet, QueryEvaluationException> bIter)
            throws QueryEvaluationException {
        return getWrappedStrategy().evaluate(tupleExpr, s, bIter);
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

    public FederatedService getService(String var1) throws QueryEvaluationException {
        return getWrappedStrategy().getService(var1);
    }


    public boolean isTrue(ValueExpr valueExpr, BindingSet bindings)
            throws ValueExprEvaluationException, QueryEvaluationException {
        return getWrappedStrategy().isTrue(valueExpr, bindings);
    }

    public void setIncludeProvenance(boolean p) { getWrappedStrategy().setIncludeProvenance(p); }

    public QueryExecutor getQueryExecutor() { return getWrappedStrategy().getQueryExecutor(); }

    public void setQueryExecutor(QueryExecutor executor) { getWrappedStrategy().setQueryExecutor(executor); }

}
