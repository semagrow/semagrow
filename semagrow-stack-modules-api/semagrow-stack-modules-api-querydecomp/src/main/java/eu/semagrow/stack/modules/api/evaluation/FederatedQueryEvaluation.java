package eu.semagrow.stack.modules.api.evaluation;

import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.TupleExpr;

/**
 * Created by angel on 7/1/14.
 */
public interface FederatedQueryEvaluation extends QueryEvaluation {

    FederatedQueryEvaluationSession createSession(TupleExpr expr, Dataset dataset, BindingSet bindings);

}
