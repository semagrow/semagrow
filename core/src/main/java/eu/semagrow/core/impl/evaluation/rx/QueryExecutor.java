package eu.semagrow.core.impl.evaluation.rx;

import org.openrdf.model.URI;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;
import org.reactivestreams.Publisher;

import java.util.List;

/**
 * Created by angel on 3/26/15.
 */
public interface QueryExecutor {

    Publisher<BindingSet> evaluate(URI endpoint, TupleExpr expr, BindingSet bindings)
            throws QueryEvaluationException;

    Publisher<BindingSet> evaluate(URI endpoint, TupleExpr expr, List<BindingSet> bindings)
            throws QueryEvaluationException;

}
