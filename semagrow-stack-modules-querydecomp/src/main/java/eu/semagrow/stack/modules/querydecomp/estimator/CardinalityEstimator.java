package eu.semagrow.stack.modules.querydecomp.estimator;

import org.openrdf.query.algebra.TupleExpr;

/**
 * Created by angel on 4/25/14.
 */
public interface CardinalityEstimator {

    public long getCardinality(TupleExpr expr);

    public long getSelectivity(TupleExpr expr);
}
