package eu.semagrow.stack.modules.sails.semagrow.optimizer;

import org.openrdf.query.algebra.TupleExpr;

import java.util.*;

/**
 * Created by angel on 9/30/14.
 */
public class PlanCollection {

    private Map<Set<TupleExpr>, Collection<Plan>> planMap;

    public PlanCollection() {
        planMap = new HashMap<Set<TupleExpr>, Collection<Plan>>();
    }

    public Set<TupleExpr> getExpressions() {
        Set<TupleExpr> allExpressions = new HashSet<TupleExpr>();
        for (Set<TupleExpr> s : planMap.keySet() )
            allExpressions.addAll(s);
        return allExpressions;
    }

    public void add(Set<TupleExpr> e, Collection<TupleExpr> plans) {
        for (TupleExpr expr : plans)
            addPlan(new Plan(e, expr));
    }

    public void addPlan(Set<TupleExpr> e, Collection<Plan> plans) {
        if (planMap.containsKey(e))
            planMap.get(e).addAll(plans);
        else
            planMap.put(e, plans);
    }

    public void addPlan(Collection<Plan> plans) {
        for (Plan p : plans)
            addPlan(p);
    }

    public void add(Set<TupleExpr> e, TupleExpr plan) {
        List<TupleExpr> plans = new LinkedList<TupleExpr>();
        plans.add(plan);
        add(e, plans);
    }

    public void addPlan(Plan plan) {
        List<Plan> plans = new LinkedList<Plan>();
        plans.add(plan);
        addPlan(plan.getPlanId(), plans);
    }

    public Collection<Plan> get(Set<TupleExpr> set) {
        if (!planMap.containsKey(set)) {
            Collection<Plan> emptyPlans = new LinkedList<Plan>();
            planMap.put(set, emptyPlans);
        }

        return planMap.get(set);
    }

}
