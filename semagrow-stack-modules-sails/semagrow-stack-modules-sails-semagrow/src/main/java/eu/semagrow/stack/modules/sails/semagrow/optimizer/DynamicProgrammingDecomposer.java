package eu.semagrow.stack.modules.sails.semagrow.optimizer;

import eu.semagrow.stack.modules.api.decomposer.QueryDecomposer;
import eu.semagrow.stack.modules.api.decomposer.QueryDecompositionException;
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

    private SourceSelector sourceSelector;

    final private Logger logger = LoggerFactory.getLogger(DynamicProgrammingDecomposer.class);

    public DynamicProgrammingDecomposer(CostEstimator estimator, SourceSelector selector) {
        costEstimator = estimator;
        sourceSelector = selector;
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

            plans.add(exprLabel, new SourceQuery(e.clone(), endpoints));
        }

        // SPLENDID also cluster statementpatterns of the same source.

        return plans;
    }

    protected void prunePlans(Collection<TupleExpr> plans) {
        // group equivalent plans
        // get the minimum-cost plan for each equivalence class

        Collection<TupleExpr> bestPlans = new ArrayList<TupleExpr>();
        Map<TupleExpr, Double> estimatedCosts = new HashMap<TupleExpr, Double>();

        boolean inComparable = true;

        for (TupleExpr candidatePlan : plans) {
            inComparable = true;
            double cost1 = costEstimator.getCost(candidatePlan);

            for (TupleExpr plan : bestPlans) {
                if (isPlanComparable(candidatePlan, plan)) {
                    inComparable = false;
                    double cost2 = estimatedCosts.get(plan);
                    if (cost1 < cost2) {
                        bestPlans.remove(plan);
                        bestPlans.add(candidatePlan);
                        estimatedCosts.put(candidatePlan, cost1);
                    }
                }
            }
            // check if plan is incomparable with all best plans yet discovered.
            if (inComparable) {
                bestPlans.add(candidatePlan);
                estimatedCosts.put(candidatePlan, cost1);
            }
        }

        int planSize = plans.size();
        plans.retainAll(bestPlans);
        logger.info("Pruned " + (planSize - plans.size()) + " suboptimal plans of " + planSize + " plans");
    }

    protected boolean isPlanComparable(TupleExpr plan1, TupleExpr plan2) {
        return true;
    }

    protected Collection<TupleExpr> joinPlans(Collection<TupleExpr> plan1, Collection<TupleExpr> plan2,
                                              Collection<ValueExpr> filterConditions) {

        Collection<TupleExpr> plans = new LinkedList<TupleExpr>();

        for (TupleExpr p1 : plan1) {
            for (TupleExpr p2 : plan2) {

                Collection<TupleExpr> joins = createPhysicalJoins(p1, p2);

                for (TupleExpr plan : joins) {
                    TupleExpr p = FilterUtils.applyRemainingFilters(plan, filterConditions);
                    plans.add(p);
                }

                TupleExpr expr = pushJoinRemote(p1, p2, filterConditions);
                if (expr != null)
                    plans.add(expr);
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

    private TupleExpr pushJoinRemote(TupleExpr e1, TupleExpr e2, Collection<ValueExpr> filterConditions) {
        if (e1 instanceof SourceQuery &&
                e2 instanceof SourceQuery) {

            SourceQuery q1 = (SourceQuery) e1;
            SourceQuery q2 = (SourceQuery) e2;
            List<URI> sources = commonSources(q1, q2);
            if (!sources.isEmpty()) {
                TupleExpr expr = FilterUtils.applyRemainingFilters(new Join(q1.getArg(), q2.getArg()), filterConditions);
                return new SourceQuery(expr, sources);
            }
        }

        return null;
    }

    private Collection<TupleExpr> createPhysicalJoins(TupleExpr e1, TupleExpr e2) {
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

    protected void finalizePlans(Collection<TupleExpr> plans) {

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

                        Collection<TupleExpr> plans1 = optPlans.get(o1);
                        Collection<TupleExpr> plans2 = optPlans.get(o2);
                        Collection<TupleExpr> newPlans = joinPlans(plans1, plans2, filterConditions);

                        optPlans.add(s, newPlans);
                    }
                }
                prunePlans(optPlans.get(s));
            }
        }
        Collection<TupleExpr> fullPlans = optPlans.get(r);
        finalizePlans(fullPlans);

        if (!fullPlans.isEmpty()) {
            logger.info("Found " + fullPlans.size()+" complete optimal plans");
            TupleExpr bestPlan = fullPlans.iterator().next();
            bgp.replaceWith(bestPlan);
        }
    }

    private static <T> Iterable<Set<T>> subsetsOf(Set<T> s, int k) {
        return new CombinationIterator(k, s);
    }

    protected class PlanCollection {

        private Map<Set<TupleExpr>, Collection<TupleExpr>> planMap;

        public PlanCollection() {
            planMap = new HashMap<Set<TupleExpr>, Collection<TupleExpr>>();
        }

        public Set<TupleExpr> getExpressions() {
            Set<TupleExpr> allExpressions = new HashSet<TupleExpr>();
            for (Set<TupleExpr> s : planMap.keySet() )
                allExpressions.addAll(s);
            return allExpressions;
        }

        public void add(Set<TupleExpr> e, Collection<TupleExpr> plans) {
            if (planMap.containsKey(e))
                planMap.get(e).addAll(plans);
            else
                planMap.put(e, plans);
        }

        public void add(Set<TupleExpr> e, TupleExpr plan) {
            List<TupleExpr> plans = new LinkedList<TupleExpr>();
            plans.add(plan);
            add(e, plans);
        }

        public Collection<TupleExpr> get(Set<TupleExpr> set) {
            if (!planMap.containsKey(set)) {
                Collection<TupleExpr> emptyPlans = new LinkedList<TupleExpr>();
                planMap.put(set, emptyPlans);
            }

            return planMap.get(set);
        }

    }
}
