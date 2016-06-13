package eu.semagrow.core.evalit;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

/**
 * Created by angel on 6/11/14.
 */
public interface QueryEvaluation {

    QueryEvaluationSession createSession(TupleExpr expr, Dataset dataset, BindingSet bindings);
}
