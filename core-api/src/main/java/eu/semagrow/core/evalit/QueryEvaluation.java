package eu.semagrow.core.evalit;

import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.TupleExpr;

/**
 * Created by angel on 6/11/14.
 */
public interface QueryEvaluation {

    QueryEvaluationSession createSession(TupleExpr expr, Dataset dataset, BindingSet bindings);
}
