package org.semagrow.plan;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.*;
import org.eclipse.rdf4j.query.algebra.evaluation.util.QueryOptimizerList;
import org.semagrow.art.Loggable;
import org.semagrow.estimator.CardinalityEstimatorResolver;
import org.semagrow.estimator.CostEstimatorResolver;
import org.semagrow.local.LocalSite;
import org.semagrow.plan.optimizer.FilterPlanOptimizer;
import org.semagrow.plan.queryblock.*;
import org.semagrow.selector.QueryAwareSourceSelector;
import org.semagrow.selector.SourceSelector;

import java.util.Collection;

/**
 * The default implementation of a {@link QueryCompiler}
 * @author acharal
 * @since 2.0
 */
public class SimpleQueryCompiler implements QueryCompiler {

    private CostEstimatorResolver costEstimatorResolver;
    private CardinalityEstimatorResolver cardinalityEstimatorResolver;
    private SourceSelector sourceSelector;

    public SimpleQueryCompiler(CostEstimatorResolver costEstimatorResolver,
                               CardinalityEstimatorResolver cardinalityEstimatorResolver,
                               SourceSelector sourceSelector)
    {
        this.costEstimatorResolver = costEstimatorResolver;
        this.cardinalityEstimatorResolver = cardinalityEstimatorResolver;
        this.sourceSelector = sourceSelector;
    }

    @Override
    @Loggable
    public Plan compile(QueryRoot query, Dataset dataset, BindingSet bindings) {

        // transformations on logical query.
        rewrite(query.getArg(), dataset, bindings);

        // split query to queryblocks.
        QueryBlock blockRoot = blockify(query, dataset, bindings);

        performSourceSelection(query);

        // infer interesting properties for each query block.
        blockRoot.visit(new InterestingPropertiesVisitor());     // infer interesting properties for each block

        // traverse Blocks and compile them bottom-up.
        Collection<Plan> plans = blockRoot.getPlans(getContext());

        // enforce Site = Local
        RequestedPlanProperties props = new RequestedPlanProperties();
        props.setSite(LocalSite.getInstance());

        plans = getContext().enforceProps(plans, props);
        getContext().prune(plans);

        Plan plan = (plans.isEmpty()) ? new Plan(new EmptySet()) : plans.iterator().next();

        optimize(plan, dataset, bindings);

        return plan;
    }

    @Loggable
    private void performSourceSelection(QueryRoot query) {
        if (sourceSelector instanceof QueryAwareSourceSelector) {
            ((QueryAwareSourceSelector) sourceSelector).processTupleExpr(query);
        }
    }

    private QueryBlock blockify(QueryRoot query, Dataset dataset, BindingSet bindings) {
        QueryBlock block = QueryBlockBuilder.build(query);   // translate TupleExpr to simple QueryBlocks
        block.visit(new DistinctStrategyVisitor());          // relax duplicate restriction if possible to facilitate merging
        block.visit(new ExistToEachQuantificationVisitor()); // try unnest existential queries if possible
        block.visit(new UnionMergeVisitor());                // try merge union blocks if possible
        block.visit(new SelectMergeVisitor());               // try merge select blocks if possible
        return block;
    }

    /**
     * Rewrites the logical expression into a logically-equivalent simpler ``canonical'' expression.
     * @param expr The expression subject to transformation. It will be substituted by the equivalent expression
     *             and therefore must have a parent.
     * @param dataset
     * @param bindings possible bindings for some of the variables in the expression.
     */
    protected void rewrite(TupleExpr expr, Dataset dataset, BindingSet bindings) {

        assert expr.getParentNode() != null;

        QueryOptimizer queryOptimizer =  new QueryOptimizerList(
                new BindingAssigner(),                  // substitute variables with constants if in the given bindingset
                new CompareOptimizer(),                 // substitute Compare with SameTerm if possible
                new SameTermFilterOptimizer(),          // rename variables or replace with constants if filtered with SameTerm
                new ConjunctiveConstraintSplitter(),    // splits Filters And to consecutive applications
                new DisjunctiveConstraintOptimizer(),   // split Filters Or to Union
                new FilterOptimizer(),                  // push Filters as deep as possible
                new QueryModelNormalizer()              // remove emptysets, singletonsets, transform to DNF (union before joins)
        );

        queryOptimizer.optimize(expr, dataset, bindings);
    }

    protected void optimize(TupleExpr expr, Dataset dataset, BindingSet bindings) {

        QueryOptimizer queryOptimizer =  new QueryOptimizerList(
                new FilterPlanOptimizer()
        );

        queryOptimizer.optimize(expr, dataset, bindings);
    }

    protected CompilerContext getContext() {
        DefaultCompilerContext context = new DefaultCompilerContext();
        context.setCardinalityEstimatorResolver(cardinalityEstimatorResolver);
        context.setCostEstimatorResolver(costEstimatorResolver);
        context.setSourceSelector(sourceSelector);
        return context;
    }
}
