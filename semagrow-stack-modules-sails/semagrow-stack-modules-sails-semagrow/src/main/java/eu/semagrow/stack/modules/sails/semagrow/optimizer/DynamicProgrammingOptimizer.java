package eu.semagrow.stack.modules.sails.semagrow.optimizer;

import eu.semagrow.stack.modules.api.ResourceSelector;
import eu.semagrow.stack.modules.api.SelectedResource;
import eu.semagrow.stack.modules.querydecomp.estimator.CostEstimator;
import eu.semagrow.stack.modules.sails.semagrow.algebra.SourceQuery;
import eu.semagrow.stack.modules.sails.semagrow.helpers.BPGCollector;
import eu.semagrow.stack.modules.sails.semagrow.helpers.CombinationIterator;
import org.openrdf.model.URI;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.QueryOptimizer;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;

import java.util.*;

/**
 * Created by angel on 3/13/14.
 */
public class DynamicProgrammingOptimizer implements QueryOptimizer {

    private CostEstimator costEstimator;
    private ResourceSelector resourceSelector;

    public DynamicProgrammingOptimizer(CostEstimator estimator, ResourceSelector selector) {
        costEstimator = estimator;
        resourceSelector = selector;
    }

    /**
     * Will extract the base expressions (i.e. relations) and create alternative access plans
     * for each one. This is a different implementation from the traditional dynamic programming
     * algorithm where the base expressions are already defined. You can override accessPlans
     * to employ a heuristic to change the notion of the base expressions used.
     * @param expr
     * @return a list of access plans.
     */
    protected PlanCollection accessPlans(TupleExpr expr) {

        PlanCollection plans = new PlanCollection();

        // extract the statement patterns
        List<StatementPattern> statementPatterns = StatementPatternCollector.process(expr);

        // extract the filter conditions of the query

        for (StatementPattern pattern : statementPatterns) {
            // get sources for each pattern
            List<SelectedResource> resources = resourceSelector.getSelectedResources(pattern, 0);

            // apply filters that can be applied to the statementpattern

            TupleExpr e = pattern;
            Set<TupleExpr> exprLabel =  new HashSet<TupleExpr>();
            exprLabel.add(e);

            // create alternative SourceQuery for each filtered-statementpattern
            for (SelectedResource r : resources) {
                plans.add(exprLabel, new SourceQuery(e, r.getEndpoint()));
            }
        }

        // SPLENDID also cluster statementpatterns of the same source.

        return plans;
    }

    protected void prunePlans(Collection<TupleExpr> plans) {
        // group equivalent plans
        // get the minimum-cost plan for each equivalence class
    }

    protected Collection<TupleExpr> joinPlans(Collection<TupleExpr> plan1, Collection<TupleExpr> plan2) {

        Collection<TupleExpr> plans = new LinkedList<TupleExpr>();

        for (TupleExpr p1 : plan1) {
            for (TupleExpr p2 : plan2) {
                TupleExpr expr = new Join(p1,p2);
                plans.add(expr);

                expr = new Join(p2,p1);
                plans.add(expr);
            }
        }
        return plans;
    }

    protected void finalizePlans() {

    }

    public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {

        Collection<TupleExpr> basicGraphPatterns = BPGCollector.process(tupleExpr);

        for (TupleExpr bgp : basicGraphPatterns)
            optimizebgp(bgp);
    }

    public void optimizebgp(TupleExpr bgp) {

        // optPlans is a function from (Set of Expressions) to (Set of Plans)

        PlanCollection optPlans = accessPlans(bgp);

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
                        Collection<TupleExpr> newPlans = joinPlans(plans1, plans2);

                        optPlans.add(s, newPlans);
                        prunePlans(optPlans.get(s));
                    }
                }
            }
        }
        Collection<TupleExpr> fullPlans = optPlans.get(r);

        if (!fullPlans.isEmpty()) {
            TupleExpr bestPlan = fullPlans.iterator().next();
            bgp.replaceWith(bestPlan);
        }
    }

    private static <T> Iterable<Set<T>> subsetsOf(Set<T> s, int k) {
        return new CombinationIterator(k, s);
    }

    public class PlanCollection {

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
