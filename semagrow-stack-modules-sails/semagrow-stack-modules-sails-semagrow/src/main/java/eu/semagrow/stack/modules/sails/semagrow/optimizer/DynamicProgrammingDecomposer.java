package eu.semagrow.stack.modules.sails.semagrow.optimizer;

import eu.semagrow.stack.modules.api.decomposer.QueryDecomposer;
import eu.semagrow.stack.modules.api.decomposer.QueryDecompositionException;
import eu.semagrow.stack.modules.api.estimator.CardinalityEstimator;
import eu.semagrow.stack.modules.api.source.SourceMetadata;
import eu.semagrow.stack.modules.api.source.SourceSelector;
import eu.semagrow.stack.modules.api.estimator.CostEstimator;
import eu.semagrow.stack.modules.sails.semagrow.algebra.BindJoin;
import eu.semagrow.stack.modules.sails.semagrow.algebra.HashJoin;
import eu.semagrow.stack.modules.sails.semagrow.algebra.SourceQuery;
import eu.semagrow.stack.modules.sails.semagrow.helpers.BPGCollector;
import eu.semagrow.stack.modules.sails.semagrow.helpers.CombinationIterator;
import eu.semagrow.stack.modules.sails.semagrow.helpers.FilterCollector;
import org.openrdf.model.URI;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.*;
import org.openrdf.query.algebra.evaluation.QueryOptimizer;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.algebra.helpers.VarNameCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


/**
 * Created by angel on 3/13/14.
 */
public class DynamicProgrammingDecomposer implements QueryDecomposer {

    private CostEstimator costEstimator;
    private CardinalityEstimator cardinalityEstimator;
    private SourceSelector sourceSelector;

    final private Logger logger = LoggerFactory.getLogger(DynamicProgrammingDecomposer.class);

    public DynamicProgrammingDecomposer(CostEstimator estimator,
                                        CardinalityEstimator cardinalityEstimator,
                                        SourceSelector selector) {
        this.costEstimator = estimator;
        this.sourceSelector = selector;
        this.cardinalityEstimator = cardinalityEstimator;
    }

    /**
     * Will extract the base expressions (i.e. relations) and create alternative access plans
     * for each one. This is a different implementation from the traditional dynamic programming
     * algorithm where the base expressions are already defined. You can override accessPlans
     * to employ a heuristic to change the notion of the base expressions used.
     * @param expr
     * @return a list of access plans.
     */
    protected Collection<Plan> accessPlans(TupleExpr expr, Dataset dataset, BindingSet bindings,
                                         DecomposerContext ctx)
        throws QueryDecompositionException {

        Collection<Plan> plans = new LinkedList<Plan>();

        // extract the statement patterns
        List<StatementPattern> statementPatterns = StatementPatternCollector.process(expr);

        // extract the filter conditions of the query

        for (StatementPattern pattern : statementPatterns) {

            // get sources for each pattern
            Collection<SourceMetadata> sources = getSources(pattern,dataset,bindings);

            // apply filters that can be applied to the statementpattern
            TupleExpr e = pattern;

            Set<TupleExpr> exprLabel =  new HashSet<TupleExpr>();
            exprLabel.add(e);

            if (sources.isEmpty())
                throw new QueryDecompositionException("No suitable sources found for statement pattern " + pattern.toString());

            List<Plan> sourcePlans = new LinkedList<Plan>();

            for (SourceMetadata sourceMetadata : sources) {
                //URI source = sourceMetadata.getEndpoints().get(0);
                //Plan p1 = createPlan(exprLabel, sourceMetadata.target(), source, ctx);
                // FIXME: Don't use always the first source.
                Plan p1 = createPlan(exprLabel, sourceMetadata.target().clone(), sourceMetadata, ctx);
                sourcePlans.add(p1);
            }

            Plan p  = createUnionPlan(sourcePlans, ctx);
            plans.add(p);
        }

        // SPLENDID also cluster statementpatterns of the same source.

        return plans;
    }

    public Plan createUnionPlan(List<Plan> plans, DecomposerContext ctx) {

        Iterator<Plan> it = plans.iterator();
        Plan p;

        if (it.hasNext())
            p = it.next();
        else
            return null;

        while (it.hasNext()) {
            Plan i = it.next();
            p = createPlan(p.getPlanId(), new Union(enforceLocalSite(p,ctx), enforceLocalSite(i,ctx)), ctx);
        }

        return p;
    }

    /**
     * Create all the ways that to join two plans
     * @param plan1
     * @param plan2
     * @return
     */
    protected Collection<Plan> joinPlans(Collection<Plan> plan1, Collection<Plan> plan2,
                                         DecomposerContext ctx) {

        Collection<Plan> plans = new LinkedList<Plan>();

        for (Plan p1 : plan1) {
            for (Plan p2 : plan2) {

                Set<String> s1 = p1.getBindingNames();
                Set<String> s2 = p2.getBindingNames();
                s1.retainAll(s2);

                if (s1.isEmpty())
                    break;

                Collection<TupleExpr> joins = createPhysicalJoins(p1, p2, ctx);
                Set<TupleExpr> s = new HashSet<TupleExpr>(p1.getPlanId());
                s.addAll(p2.getPlanId());

                for (TupleExpr plan : joins) {
                    //TupleExpr e = applyRemainingFilters(plan, ctx.filters);
                    Plan p = createPlan(s, plan, ctx);
                    plans.add(p);
                }

                Plan expr = pushJoinRemote(p1, p2, ctx);
                if (expr != null) {
                    //Plan p = createPlan(s,expr,ctx);
                    //plans.add(p);
                    plans.add(expr);
                }
            }
        }
        return plans;
    }

    /**
     * Prune suboptimal plans
     * @param plans
     */
    protected void prunePlans(Collection<Plan> plans) {
        // group equivalent plans
        // get the minimum-cost plan for each equivalence class

        List<Plan> bestPlans = new ArrayList<Plan>();

        boolean inComparable;

        for (Plan candidatePlan : plans) {
            inComparable = true;


            ListIterator<Plan> pIter = bestPlans.listIterator();
            while (pIter.hasNext()) {
                Plan plan = pIter.next();

                int plan_comp = comparePlan(candidatePlan, plan);

                if (plan_comp != 0)
                    inComparable = false;

                if (plan_comp == -1) {
                    pIter.remove();
                    pIter.add(candidatePlan);
                }
            }

            // check if plan is incomparable with all best plans yet discovered.
            if (inComparable)
                bestPlans.add(candidatePlan);
        }

        int planSize = plans.size();
        plans.retainAll(bestPlans);
        logger.info("Pruned " + (planSize - plans.size()) + " suboptimal plans of " + planSize + " plans");
    }

    protected void finalizePlans(Collection<Plan> plans, DecomposerContext ctx) {
        Collection<Plan> modified = new LinkedList<Plan>();
        for (Plan p : plans) {
            Plan p2 = enforceLocalSite(p, ctx);
            if (p2 != p) {
                modified.add(p2);
                plans.remove(p);
            }
        }

        plans.addAll(modified);
    }

    private Collection<TupleExpr> createPhysicalJoins(Plan e1, Plan e2, DecomposerContext ctx) {
        Collection<TupleExpr> plans = new LinkedList<TupleExpr>();

        TupleExpr expr;

        //TupleExpr expr = new Join(e1, e2);
        //if (!e2.getSite().equals(Plan.LOCAL)) {
        // FIXME
        if (e2.getPlanId().size() == 1) {

            if (e1.getPlanId().size() == 2 && !e1.getSite().equals(Plan.LOCAL)) {
                int i = 0;
                i++;
            }


            expr = new BindJoin(enforceLocalSite(e1, ctx), enforceLocalSite(e2, ctx));
            plans.add(expr);

        }

        /*
        expr = new HashJoin(enforceLocalSite(e1, ctx), enforceLocalSite(e2, ctx));
        plans.add(expr);
        */
        //expr = new Join(e2, e1);
        //plans.add(expr);

        return plans;
    }

    private Plan pushJoinRemote(Plan e1, Plan e2, DecomposerContext ctx) {

        URI site1 = e1.getSite();
        URI site2 = e2.getSite();

        if (site1.equals(site2) && !site1.equals(Plan.LOCAL)) {
            Set<TupleExpr> planid = new HashSet<TupleExpr>(e1.getPlanId());
            planid.addAll(e2.getPlanId());
            return createPlan(planid, new Join(e1,e2), site1, ctx);
        }

        return null;
    }

    /**
     * Update the properties of a plan
     * @param plan
     */
    protected void updatePlan(Plan plan, DecomposerContext ctx) {
        TupleExpr innerExpr = plan.getArg();

        // apply filters that can be applied
        TupleExpr e = applyRemainingFilters(innerExpr.clone(), ctx.filters);

        // update cardinality and cost properties
        plan.setCost(costEstimator.getCost(e, plan.getSite()));
        plan.setCardinality(cardinalityEstimator.getCardinality(e, plan.getSite()));

        // update site

        // update ordering

        plan.getArg().replaceWith(e);
        //FIXME: update ordering, limit, distinct, group by
    }

    protected Plan createPlan(Set<TupleExpr> planId, TupleExpr innerExpr, DecomposerContext ctx) {
        Plan p = new Plan(planId, innerExpr);
        updatePlan(p, ctx);
        return p;
    }

    protected Plan createPlan(Set<TupleExpr> planId, TupleExpr innerExpr, URI source, DecomposerContext ctx) {
        Plan p = new Plan(planId, innerExpr);
        p.setSite(source);
        updatePlan(p, ctx);
        return p;
    }

    protected Plan createPlan(Set<TupleExpr> planId, TupleExpr innerExpr,
                              SourceMetadata metadata, DecomposerContext ctx)
    {
        URI source = metadata.getEndpoints().get(0);
        Plan p = new Plan(planId, innerExpr);
        p.setSite(source);

        Set<String> varNames = innerExpr.getBindingNames();
        for (String varName : varNames) {
            Collection<URI> schemas = metadata.getSchema(varName);
            if (!schemas.isEmpty())
                p.setSchemas(varName, schemas);
        }

        updatePlan(p, ctx);
        return p;
    }


    private boolean isPlanComparable(Plan plan1, Plan plan2) {
        // FIXME: take plan properties into account
        return plan1.getSite().equals(plan2.getSite());
    }

    /**
     * Compare two plans; can be partial order of plans.
     * In order to be compared, both cost and properties of the plans
     * must be comparable.
     * @param plan1
     * @param plan2
     * @return 0 if plan are equal or uncomparable
     *         -1 if plan1 is better than plan2
     *         1  if plan2 is better than plan1
     */
    private int comparePlan(Plan plan1, Plan plan2) {
        if (isPlanComparable(plan1, plan2)) {
            return plan1.getCost() < plan2.getCost() ? -1 : 1;
        } else {
            return 0;
        }
    }

    /**
     * Choose one of the plans as best
     * @param plans
     * @return
     */
    private TupleExpr getBestPlan(Collection<Plan> plans) {
        if (plans.isEmpty())
            return null;

        Plan bestPlan = plans.iterator().next();

        for (Plan p : plans)
            if (p.getCost() < bestPlan.getCost())
                bestPlan = p;

        return bestPlan;
    }

    private Plan enforceLocalSite(Plan p, DecomposerContext ctx) {
        if (p.getSite() == Plan.LOCAL)
            return p;
        else
            return createPlan(p.getPlanId(), new SourceQuery(p, p.getSite()), Plan.LOCAL, ctx);
    }

    protected Collection<SourceMetadata> getSources(StatementPattern pattern, Dataset dataset, BindingSet bindings) {
        return sourceSelector.getSources(pattern,dataset,bindings);
    }

    protected DecomposerContext getContext(TupleExpr bgp, Dataset dataset, BindingSet bindings) {
        DecomposerContext ctx = new DecomposerContext();
        ctx.filters = FilterCollector.process(bgp);
        return ctx;
    }

    private static <T> Iterable<Set<T>> subsetsOf(Set<T> s, int k) {
        return new CombinationIterator<T>(k, s);
    }

    public static Collection<ValueExpr> getRelevantFiltersConditions(TupleExpr e, Collection<ValueExpr> filterConditions) {
        Set<String> variables = VarNameCollector.process(e);
        Collection<ValueExpr> relevantConditions = new LinkedList<ValueExpr>();

        for (ValueExpr condition : filterConditions) {
            Set<String> conditionVariables = VarNameCollector.process(condition);
            if (variables.containsAll(conditionVariables))
                relevantConditions.add(condition);
        }

        return relevantConditions;
    }

    private static TupleExpr applyFilters(TupleExpr e, Collection<ValueExpr> conditions) {
        TupleExpr expr = e;

        for (ValueExpr condition : conditions)
            expr = new Filter(expr, condition);

        return expr;
    }

    public static TupleExpr applyRemainingFilters(TupleExpr e, Collection<ValueExpr> conditions) {
        Collection<ValueExpr> filtersApplied = FilterCollector.process(e);
        Collection<ValueExpr> remainingFilters = getRelevantFiltersConditions(e, conditions);
        remainingFilters.removeAll(filtersApplied);
        return applyFilters(e, remainingFilters);
    }

    public void decompose(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings)
            throws QueryDecompositionException {

        Collection<TupleExpr> basicGraphPatterns = BPGCollector.process(tupleExpr);

        for (TupleExpr bgp : basicGraphPatterns)
            decomposebgp(bgp, dataset, bindings);

        QueryOptimizer finalizeOptimizers = new LimitPushDownOptimizer();
        finalizeOptimizers.optimize(tupleExpr, dataset, bindings);
    }

    public void decomposebgp(TupleExpr bgp, Dataset dataset, BindingSet bindings)
            throws QueryDecompositionException {

        DecomposerContext ctx = getContext(bgp, dataset, bindings);

        // optPlans is a function from (Set of Expressions) to (Set of Plans)
        PlanCollection optPlans = new PlanCollection();

        Collection<Plan> accessPlans = accessPlans(bgp, dataset, bindings, ctx);

        optPlans.addPlan(accessPlans);

        // plans.getExpressions() get basic expressions
        // subsets S of size i
        //
        Set<TupleExpr> r = optPlans.getExpressions();

        int count = r.size();

        // bottom-up starting for subplans of size "k"
        for (int k = 2; k <= count; k++) {

            // enumerate all subsets of r of size k
            for (Set<TupleExpr> s : subsetsOf(r, k)) {

                for (int i = 1; i < k; i++) {

                    // let disjoint sets o1 and o2 such that s = o1 union o2
                    for (Set<TupleExpr> o1 : subsetsOf(s, i)) {

                        Set<TupleExpr> o2 = new HashSet<TupleExpr>(s);
                        o2.removeAll(o1);

                        Collection<Plan> plans1 = optPlans.get(o1);
                        Collection<Plan> plans2 = optPlans.get(o2);
                        Collection<Plan> newPlans = joinPlans(plans1, plans2, ctx);

                        optPlans.addPlan(newPlans);
                    }
                }
                prunePlans(optPlans.get(s));
            }
        }
        Collection<Plan> fullPlans = optPlans.get(r);
        finalizePlans(fullPlans, ctx);

        if (!fullPlans.isEmpty()) {
            logger.info("Found " + fullPlans.size()+" complete optimal plans");
            TupleExpr bestPlan = getBestPlan(fullPlans);
            bgp.replaceWith(bestPlan);
        }
    }
}
