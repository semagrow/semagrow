package eu.semagrow.stack.modules.querydecomp.estimator;

import org.openrdf.model.URI;
import org.openrdf.query.algebra.TupleExpr;

/**
 * Created by angel on 4/25/14.
 */
public interface CardinalityEstimator {

    long getCardinality(TupleExpr expr);

    long getCardinality(TupleExpr expr, URI source);
}
