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
import eu.semagrow.stack.modules.sails.semagrow.helpers.FilterUtils;
import org.openrdf.model.URI;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.*;
import org.openrdf.query.algebra.evaluation.QueryOptimizer;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
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
    protected PlanCollection accessPlans(TupleExpr expr, Dataset dataset, BindingSet bindings,
                                         Collection<ValueExpr> filterConditions)
        throws QueryDecompositionException {

        PlanCollection plans = new PlanCollection();

        // extract the statement patterns
        List<StatementPattern> statementPatterns = StatementPatternCollector.process(expr);

        // extract the filter conditions of the query

        for (StatementPattern pattern : statementPatterns) {
            // get sources for each pattern
            Collection<SourceMetadata> sources = getSources(pattern,dataset,bindings);

            // apply filters that can be applied to the statementpattern
            TupleExpr e = FilterUtils.applyRemainingFilters(pattern, filterConditions);

            Set<TupleExpr> exprLabel =  new HashSet<TupleExpr>();
            exprLabel.add(e);

            if (sources.isEmpty())
                throw new QueryDecompositionException("No suitable sources found for statement pattern " + pattern.toString());


            // create alternative SourceQuery for each filtered-statementpattern
            List<URI> endpoints = new LinkedList<URI>();
            for (SourceMetadata sourceMetadata : sources) {
                if (sourceMetadata.getEndpoints().size() > 0)
                    endpoints.add(sourceMetadata.getEndpoints().get(0));
            }

            e = new SourceQuery(e.clone(), endpoints);
            Plan p = new Plan(exprLabel, e);
            p.setCost(costEstimator.getCost(e));
            p.setCardinality(cardinalityEstimator.getCardinality(e));
            plans.addPlan(p);
        }

        // SPLENDID also cluster statementpatterns of the same source.

        return plans;
    }

    protected void prunePlans(Collection<Plan> plans) {
        // group equivalent plans
        // get the minimum-cost plan for each equivalence class

        Collection<Plan> bestPlans = new ArrayList<Plan>();

        boolean inComparable = true;

        for (Plan candidatePlan : plans) {
            inComparable = true;
            double cost1 = candidatePlan.getCost();

            for (Plan plan : bestPlans) {
                if (isPlanComparable(candidatePlan, plan)) {
                    inComparable = false;
                    double cost2 = plan.getCost();
                    if (cost1 < cost2) {
                        bestPlans.remove(plan);
                        bestPlans.add(candidatePlan);
                    }
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

    protected boolean isPlanComparable(Plan plan1, Plan plan2) {
        return true;
    }

    protected Collection<Plan> joinPlans(Collection<Plan> plan1, Collection<Plan> plan2,
                                              Collection<ValueExpr> filterConditions) {

        Collection<Plan> plans = new LinkedList<Plan>();

        for (Plan p1 : plan1) {
            for (Plan p2 : plan2) {

                Collection<TupleExpr> joins = createPhysicalJoins(p1, p2);
                Set<TupleExpr> s = new HashSet<TupleExpr>(p1.getPlanId());
                s.addAll(p2.getPlanId());

                for (TupleExpr plan : joins) {
                    TupleExpr e = FilterUtils.applyRemainingFilters(plan, filterConditions);
                    Plan p = new Plan(s,e);
                    p.setCost(costEstimator.getCost(e));
                    p.setCardinality(cardinalityEstimator.getCardinality(e));
                    plans.add(p);
                }

                TupleExpr expr = pushJoinRemote(p1, p2, filterConditions);
                if (expr != null) {
                    Plan p = new Plan(s,expr);
                    p.setCost(costEstimator.getCost(expr));
                    p.setCardinality(cardinalityEstimator.getCardinality(expr));
                    plans.add(p);
                }
            }
        }
        return plans;
    }

    private List<URI> commonSources(SourceQuery e1, SourceQuery e2) {
        List<URI> commonURIs = new LinkedList<URI>(e1.getSources());
        commonURIs.retainAll(e2.getSources());
        return commonURIs;
    }

    protected Collection<SourceMetadata> getSources(StatementPattern pattern, Dataset dataset, BindingSet bindings) {
        return sourceSelector.getSources(pattern,dataset,bindings);
    }

    private TupleExpr pushJoinRemote(Plan e1, Plan e2, Collection<ValueExpr> filterConditions) {
        if (e1.getArg() instanceof SourceQuery &&
                e2.getArg() instanceof SourceQuery) {

            SourceQuery q1 = (SourceQuery) e1.getArg();
            SourceQuery q2 = (SourceQuery) e2.getArg();
            List<URI> sources = commonSources(q1, q2);
            if (!sources.isEmpty()) {
                TupleExpr expr = FilterUtils.applyRemainingFilters(new Join(q1.getArg(), q2.getArg()), filterConditions);
                return new SourceQuery(expr, sources);
            }
        }

        return null;
    }

    private Collection<TupleExpr> createPhysicalJoins(Plan e1, Plan e2) {
        Collection<TupleExpr> plans = new LinkedList<TupleExpr>();

        //TupleExpr expr = new Join(e1, e2);
        TupleExpr expr = new BindJoin(e1,e2);
        plans.add(expr);

        expr = new HashJoin(e1,e2);
        plans.add(expr);

        //expr = new Join(e2, e1);
        //plans.add(expr);

        return plans;
    }

    protected void finalizePlans(Collection<Plan> plans) {

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

        Collection<ValueExpr> filterConditions = FilterCollector.process(bgp);

        // optPlans is a function from (Set of Expressions) to (Set of Plans)

        PlanCollection optPlans = accessPlans(bgp, dataset, bindings, filterConditions);

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
                        Collection<Plan> newPlans = joinPlans(plans1, plans2, filterConditions);

                        optPlans.addPlan(newPlans);
                    }
                }
                prunePlans(optPlans.get(s));
            }
        }
        Collection<Plan> fullPlans = optPlans.get(r);
        finalizePlans(fullPlans);

        if (!fullPlans.isEmpty()) {
            logger.info("Found " + fullPlans.size()+" complete optimal plans");
            TupleExpr bestPlan = fullPlans.iterator().next();
            bgp.replaceWith(bestPlan);
        }
    }

    private static <T> Iterable<Set<T>> subsetsOf(Set<T> s, int k) {
        return new CombinationIterator<T>(k, s);
    }

}
