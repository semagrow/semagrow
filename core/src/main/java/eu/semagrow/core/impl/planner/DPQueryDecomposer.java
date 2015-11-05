package eu.semagrow.core.impl.planner;

import eu.semagrow.core.decomposer.QueryDecomposer;
import eu.semagrow.core.estimator.CardinalityEstimator;
import eu.semagrow.core.impl.optimizer.LimitPushDownOptimizer;
import eu.semagrow.core.impl.selector.StaticSourceSelector;
import eu.semagrow.core.impl.optimizer.ExtensionOptimizer;
import eu.semagrow.core.source.SourceSelector;
import eu.semagrow.core.impl.estimator.CostEstimator;
import eu.semagrow.core.impl.util.BPGCollector;
import eu.semagrow.core.impl.util.FilterCollector;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.QueryOptimizer;
import org.openrdf.query.algebra.evaluation.util.QueryOptimizerList;

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
    {

        Collection<TupleExpr> basicGraphPatterns = BPGCollector.process(expr);

        for (TupleExpr bgp : basicGraphPatterns)
            decomposebgp(bgp, dataset, bindings);

        QueryOptimizer opt =  new QueryOptimizerList(
                new ExtensionOptimizer(),
                new LimitPushDownOptimizer());

        opt.optimize(expr, dataset, bindings);
    }

    public void decomposebgp(TupleExpr bgp, Dataset dataset, BindingSet bindings)
    {
        DecomposerContext ctx = getContext(bgp, dataset, bindings);

        SourceSelector staticSelector = new StaticSourceSelector(sourceSelector.getSources(bgp, dataset, bindings));

        PlanGenerator planGenerator = new PlanGeneratorImpl(ctx, staticSelector, costEstimator, cardinalityEstimator);

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
