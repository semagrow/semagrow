package eu.semagrow.core.impl.rx;

import org.openrdf.model.URI;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;
import org.reactivestreams.Publisher;

/**
 * Created by angel on 3/26/15.
 */
public interface ReactiveQueryExecutor {

    Publisher<BindingSet> evaluateReactive(URI endpoint, TupleExpr expr, BindingSet bindings)
            throws QueryEvaluationException;

    Publisher<BindingSet> evaluateReactive(URI endpoint, TupleExpr expr, Publisher<BindingSet> bindings)
            throws QueryEvaluationException;

    int getBatchSize();

    void setBatchSize(int b);
}
