package eu.semagrow.stack.modules.sails.semagrow.estimator;

import eu.semagrow.stack.modules.api.ResourceSelector;
import eu.semagrow.stack.modules.querydecomp.estimator.CardinalityEstimator;
import eu.semagrow.stack.modules.querydecomp.estimator.CostEstimator;
import eu.semagrow.stack.modules.sails.semagrow.algebra.SourceQuery;
import org.openrdf.query.algebra.TupleExpr;

/**
 * Created by angel on 4/28/14.
 */
public class CostEstimatorImpl implements CostEstimator {

    private CardinalityEstimator cardinalityEstimator;
    private ResourceSelector resourceSelector;

    public CostEstimatorImpl(CardinalityEstimator cardinalityEstimator,
                             ResourceSelector resourceSelector) {

        this.cardinalityEstimator = cardinalityEstimator;
        this.resourceSelector = resourceSelector;
    }

    public long estimateCost(TupleExpr expr) {

        // just favor remote queries.
        if (expr instanceof SourceQuery)
            return 0;
        else
            return 1;
    }
}
