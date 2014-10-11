package eu.semagrow.stack.modules.api.estimator;

import org.openrdf.model.URI;
import org.openrdf.query.algebra.TupleExpr;

/**
 * Created by angel on 4/25/14.
 */
public interface CostEstimator {

    public double getCost(TupleExpr expr);

    public double getCost(TupleExpr expr, URI source);

}
