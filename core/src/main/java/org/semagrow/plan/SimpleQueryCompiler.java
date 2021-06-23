package org.semagrow.plan;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.*;
import org.eclipse.rdf4j.query.algebra.evaluation.util.QueryOptimizerList;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.semagrow.art.LogUtils;
import org.semagrow.art.Loggable;
import org.semagrow.estimator.CardinalityEstimatorResolver;
import org.semagrow.estimator.CostEstimatorResolver;
import org.semagrow.local.LocalSite;
import org.semagrow.plan.optimizer.BindJoinExtensionOptimizer;
import org.semagrow.plan.optimizer.FilterPlanOptimizer;
import org.semagrow.plan.queryblock.*;
import org.semagrow.plan.util.EndpointCollector;
import org.semagrow.selector.QueryAwareSourceSelector;
import org.semagrow.selector.SourceSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * The default implementation of a {@link QueryCompiler}
 * @author acharal
 * @since 2.0
 */
public class SimpleQueryCompiler implements QueryCompiler {

    protected final Logger logger = LoggerFactory.getLogger(SimpleQueryCompiler.class);

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

        long t1 = System.currentTimeMillis();

        sourceSelection(query);

        long t2 = System.currentTimeMillis();

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

        addMissingProjectionElems(plan, query);
        optimize(plan, dataset, bindings);

        long t3 = System.currentTimeMillis();

        String compilationReport = "" +
                "Source Selection Time: " + (t2-t1) + " - " +
                "Compile Time: " + (t3-t2) + " - " +
                "Sources: " + EndpointCollector.process(plan).size();
        LogUtils.appendKobeReport(compilationReport);
        
        logger.info(compilationReport);

        return plan;
    }

    @Loggable
    private void sourceSelection(QueryRoot query) {
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
                new FilterPlanOptimizer(),
                new BindJoinExtensionOptimizer()
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

    private void addMissingProjectionElems(Plan plan, QueryRoot query) {
        try {
            final List<ProjectionElem> elems = new ArrayList<>();
            query.visit(new AbstractQueryModelVisitor<Exception>() {
                @Override
                public void meet(ProjectionElem node) throws Exception {
                    elems.add(node);
                }
            });
            plan.visit(new AbstractPlanVisitor<Exception>() {
                @Override
                public void meet(ProjectionElem node) throws Exception {
                    elems.remove(node);
                }
            });
            plan.visit(new AbstractPlanVisitor<Exception>() {
                @Override
                public void meet(ProjectionElemList node) throws Exception {
                    node.addElements(elems);
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
