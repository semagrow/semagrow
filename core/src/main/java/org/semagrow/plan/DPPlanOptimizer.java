package org.semagrow.plan;

import org.semagrow.art.Loggable;
import org.semagrow.plan.util.CombinationIterator;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

import java.util.*;

/**
 * A plan optimizer that uses Dynamic Programming to
 * search for an optimal plan with respect to a CostEstimator
 * @author Angelos Charalambidis
 */
public class DPPlanOptimizer
{

    final private org.slf4j.Logger logger =
    		org.slf4j.LoggerFactory.getLogger( DPPlanOptimizer.class );

    private PlanGenerator planGenerator;

    private PlanProperties properties = PlanProperties.defaultProperties();

    public DPPlanOptimizer(PlanGenerator planGenerator)
    {
        this.planGenerator = planGenerator;
    }

    /**
     * Removes from the collection all the plans that
     * are considered inferior. After pruning the plans
     * in the collection are considered incomparable and minimal
     * @param plans
     */
    protected void prunePlans(Collection<Plan> plans)
    {
        List<Plan> bestPlans = new ArrayList<Plan>();

        boolean inComparable;

        for (Plan candidatePlan : plans) {
            inComparable = true;

            ListIterator<Plan> pIter = bestPlans.listIterator();
            while (pIter.hasNext()) {
                Plan plan = pIter.next();

                if (isPlanComparable(candidatePlan, plan)) {
                    inComparable = false;
                    int plan_comp = comparePlan(candidatePlan, plan);


                    if (plan_comp != 0)
                        inComparable = false;

                    if (plan_comp < 1) {
                        pIter.remove();
                        pIter.add(candidatePlan);
                    }
                }
            }

            // check if plan is incomparable with all best plans yet discovered.
            if (inComparable)
                bestPlans.add(candidatePlan);
        }

        int planSize = plans.size();
        plans.retainAll(bestPlans);
        logger.debug("Prune {} of {} plans", (planSize - plans.size()), planSize);
    }

    protected int comparePlan(Plan plan1, Plan plan2)
    {
        if (isPlanComparable(plan1, plan2)) {
            return plan1.getProperties().getCost().compareTo(plan2.getProperties().getCost());
        } else {
            return 0;
        }
    }

    private boolean isPlanComparable(Plan plan1, Plan plan2) {
        // FIXME: take plan properties into account
        return  plan1.getProperties().isComparable(plan2.getProperties());
    }

    @Loggable
    public Optional<Plan> getBestPlan(Collection<TupleExpr> exprs, BindingSet bindings, Dataset dataset) {

        Map<Set<Integer>,PlanCollection> optPlans = new HashMap<>();
        // optPlans is a function from (Set of Expressions) to (Set of Plans)

        Integer currentId = 1;
        Set<Integer> r = new HashSet<>();

        for (TupleExpr a : exprs) {
            Set<Integer> id = Collections.singleton(currentId);
            PlanCollection p = planGenerator.accessPlans(a, bindings, dataset);
            optPlans.put(id, p);
            r.addAll(id);
            currentId++;
        }

        for (Pair<Set<Integer>, Set<Integer>> p : pairsubsets(r)) {
            Set<Integer> o1 = p.getFirst();
            Set<Integer> o2 = p.getSecond();
            Set<Integer> s = new HashSet<>(o1);
            s.addAll(o2);

            PlanCollection plans1 = optPlans.get(o1);
            PlanCollection plans2 = optPlans.get(o2);

            PlanCollection newPlans = planGenerator.joinPlans(plans1, plans2);

            optPlans.put(s, newPlans);

            prunePlans(optPlans.get(s));
        }

        PlanCollection fullPlans = optPlans.get(r);
        fullPlans = planGenerator.finalizePlans(fullPlans, properties);
        prunePlans(fullPlans);

        if (!fullPlans.isEmpty()) {
            logger.info("Found {} complete optimal plans", fullPlans.size());
        } else {
            logger.warn("Found no complete plans");
        }

        return getBestPlan(fullPlans);
    }

    /**
     * Returns the best plan out of a collection of plans
     * @param plans a collection of equivalent plans
     * @return the preferred plan
     */
    private Optional<Plan> getBestPlan(Collection<Plan> plans)
    {
        if (plans.isEmpty())
            return Optional.empty();

        Plan bestPlan = plans.iterator().next();

        for (Plan p : plans)
            if (p.getProperties().getCost().compareTo(bestPlan.getProperties().getCost()) == -1)
                bestPlan = p;

        return Optional.of(bestPlan);
    }

    private static <T> Iterable<Pair<Set<T>, Set<T>>> pairsubsets(Set<T> s) {
        return new PairSubsetIterator<T>(s);
    }

    static private class Pair<A, B> {

        final private A first;

        final private B second;

        public Pair(A first, B second)
        {
            this.first = first;
            this.second = second;
        }


        public A getFirst() { return first; }

        public B getSecond() { return second; }

        public String toString() { return "(" + first.toString() + ", " + second.toString() + ")"; }


    }

    static public class SubsetsIterator<T> implements Iterator<Set<T>> {

        private final Set<T> elements;
        private int k = 0;

        private Iterator<Set<T>> current;

        public SubsetsIterator(Set<T> elements) {
            this.elements = elements;
            k = 0;
        }

        public SubsetsIterator(Set<T> elements, int k)
        {
            this(elements);
            this.k = k - 1;
            assert k > 0;
            assert k <= elements.size();
        }

        public boolean hasNext() {
            return k <= elements.size();
        }

        public Set<T> next() {

            if (k > this.elements.size())
                return null;

            if (current == null || !current.hasNext()) {
                k++;

                if (k == this.elements.size()) {
                    k++;
                    return elements;
                }

                current = new CombinationIterator<>(k, elements);
            }

            return current.next();
        }
    }

    static public class PairSubsetIterator<T> implements Iterator<Pair<Set<T>, Set<T>>>, Iterable<Pair<Set<T>,Set<T>>>  {
        private Set<T> items;

        private Iterator<Set<T>> outer;
        private Iterator<Set<T>> inner;

        private Set<T> outerCurrent;
        private Pair<Set<T>,Set<T>> n;


        public PairSubsetIterator(Set<T> items) {
            this.items = items;
            init();
        }

        public void init() {
            if (items.size() < 2) {
                n = null;
            } else {
                outer = new SubsetsIterator<T>(items, 2);
                n = getNext();
            }
        }

        @Override
        public boolean hasNext() {
            return n != null;
        }

        @Override
        public Pair<Set<T>, Set<T>> next() {
            Pair<Set<T>,Set<T>> nn = n;
            n = getNext();
            return nn;
        }


        protected Pair<Set<T>, Set<T>> getNext()
        {
            while (true)
            {

                if (inner == null || !inner.hasNext()) {

                    if (!outer.hasNext())
                        return null;
                    else
                    {
                        // get next outerCurrent, init inner
                        outerCurrent = outer.next();
                        inner = new SubsetsIterator<T>(outerCurrent);
                    }
                }

                if (inner.hasNext()) {
                    Set<T> i = inner.next();
                    Pair<Set<T>,Set<T>> p =  getPair(outerCurrent, i);

                    if (p != null)
                        return p;
                }
            }

        }

        private Pair<Set<T>,Set<T>> getPair(Set<T> full, Set<T> part)
        {
            Set<T> s = new HashSet<>(full);
            s.removeAll(part);
            if (s.isEmpty())
                return null;
            else
                return new Pair<>(s, part);
        }

        @Override
        public Iterator<Pair<Set<T>, Set<T>>> iterator() {
            return this;
        }
    }

}
