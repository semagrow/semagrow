package org.semagrow.evaluation;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.reactivestreams.Publisher;

/**
 * Created by angel on 3/26/15.
 */
public interface EvaluationStrategy {

    Publisher<BindingSet> evaluate(TupleExpr expr, BindingSet bindings)
            throws QueryEvaluationException;

    Value evaluate(ValueExpr expr, BindingSet bindings)
            throws ValueExprEvaluationException, QueryEvaluationException;

    boolean isTrue(ValueExpr expr, BindingSet bindings)
            throws ValueExprEvaluationException, QueryEvaluationException;

}
