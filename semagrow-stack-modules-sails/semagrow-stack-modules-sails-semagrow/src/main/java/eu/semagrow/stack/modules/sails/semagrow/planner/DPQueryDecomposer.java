package eu.semagrow.stack.modules.sails.semagrow.planner;

import eu.semagrow.stack.modules.api.decomposer.QueryDecomposer;
import eu.semagrow.stack.modules.api.decomposer.QueryDecompositionException;
import eu.semagrow.stack.modules.api.estimator.CardinalityEstimator;
import eu.semagrow.stack.modules.api.source.SourceSelector;
import eu.semagrow.stack.modules.sails.semagrow.estimator.CostEstimator;
import eu.semagrow.stack.modules.sails.semagrow.helpers.BPGCollector;
import eu.semagrow.stack.modules.sails.semagrow.helpers.FilterCollector;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.TupleExpr;

import java.util.Collection;


/**
 * Created by angel on 27/4/2015.
 */
public class DPQueryDecomposer implements QueryDecomposer {

    private CostEstimator costEstimator;
    private CardinalityEstimator cardinalityEstimator;
    private SourceSelector sourceSelector;

    public DPQueryDecomposer(CostEstimator estimator,
                             CardinalityEstimator cardinalityEstimator,
                             SourceSelector selector)
    {
        this.costEstimator = estimator;
        this.sourceSelector = selector;
        this.cardinalityEstimator = cardinalityEstimator;
    }

    @Override
    public void decompose(TupleExpr expr, Dataset dataset, BindingSet bindings)
            throws QueryDecompositionException
    {

        Collection<TupleExpr> basicGraphPatterns = BPGCollector.process(expr);

        for (TupleExpr bgp : basicGraphPatterns)
            decomposebgp(bgp, dataset, bindings);
    }

    public void decomposebgp(TupleExpr bgp, Dataset dataset, BindingSet bindings)
    {
        DecomposerContext ctx = getContext(bgp, dataset, bindings);

        PlanGenerator planGenerator = new PlanGeneratorImpl(ctx, sourceSelector, costEstimator, cardinalityEstimator);

        PlanOptimizer planOptimizer = new DPPlanOptimizer(planGenerator);

        Plan bestPlan = planOptimizer.getBestPlan(bgp, bindings, dataset);
        bgp.replaceWith(bestPlan);
    }

    protected DecomposerContext getContext(TupleExpr bgp, Dataset dataset, BindingSet bindings) {
        DecomposerContext ctx = new DecomposerContext();
        ctx.filters = FilterCollector.process(bgp);
        return ctx;
    }

}
