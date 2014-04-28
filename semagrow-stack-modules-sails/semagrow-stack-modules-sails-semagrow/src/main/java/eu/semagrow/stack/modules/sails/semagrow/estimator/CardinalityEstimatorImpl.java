package eu.semagrow.stack.modules.sails.semagrow.estimator;

import eu.semagrow.stack.modules.api.ResourceSelector;
import eu.semagrow.stack.modules.querydecomp.estimator.CardinalityEstimator;
import org.openrdf.query.algebra.TupleExpr;

/**
 * Created by angel on 4/28/14.
 */
public class CardinalityEstimatorImpl implements CardinalityEstimator {

    private ResourceSelector selector;

    public CardinalityEstimatorImpl(ResourceSelector selector) {
        this.selector = selector;
    }

    public long getCardinality(TupleExpr expr) {
        return 0;
    }

    public long getSelectivity(TupleExpr expr) {
        return 0;
    }
}
