package eu.semagrow.stack.modules.api.estimator;

import org.openrdf.model.URI;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.ValueExpr;

/**
 * Created by angel on 10/1/14.
 */
public interface SelectivityEstimator {

    double getJoinSelectivity(Join expr, URI source);

    double getJoinSelectivity(Join expr);

    double getVarSelectivity(String varName, TupleExpr expr, URI source);

    double getConditionSelectivity(ValueExpr condition, TupleExpr expr, URI source);

    double getConditionSelectivity(ValueExpr condition, TupleExpr expr);
}
