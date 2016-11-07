package org.semagrow.plan;

import org.eclipse.rdf4j.query.algebra.OrderElem;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.semagrow.estimator.CardinalityEstimatorResolver;
import org.semagrow.estimator.CostEstimatorResolver;
import org.semagrow.local.LocalSite;
import org.semagrow.plan.operators.SourceQuery;
import org.semagrow.selector.Site;
import org.semagrow.selector.SourceSelector;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * @author acharal
 */
public class DefaultCompilerContext implements CompilerContext {

    private CostEstimatorResolver costEstimatorResolver;

    private CardinalityEstimatorResolver cardinalityEstimatorResolver;

    private SourceSelector sourceSelector;

    public CostEstimatorResolver getCostEstimatorResolver() { return costEstimatorResolver; }

    public void setCostEstimatorResolver(CostEstimatorResolver costEstimatorResolver) {
        this.costEstimatorResolver = costEstimatorResolver;
    }

    public CardinalityEstimatorResolver getCardinalityEstimatorResolver() { return cardinalityEstimatorResolver; }

    public void setCardinalityEstimatorResolver(CardinalityEstimatorResolver cardinalityEstimatorResolver) {
        this.cardinalityEstimatorResolver = cardinalityEstimatorResolver;
    }

    public SourceSelector getSourceSelector() { return sourceSelector; }

    public void setSourceSelector(SourceSelector sourceSelector) { this.sourceSelector = sourceSelector; }

    public Plan asPlan(TupleExpr physicalExpr, PlanProperties initProps) {

        Plan p = new Plan(physicalExpr);

       PlanProperties props = PlanPropertiesUpdater.process(physicalExpr, initProps);

        if (props.getSite() == null)
            props.setSite(LocalSite.getInstance());

        if (props.getDataProperties() == null)
            props.setDataProperties(new DataProperties());

        Site s = props.getSite();

        cardinalityEstimatorResolver.resolve(s)
                .ifPresent(ce -> props.setCardinality(ce.getCardinality(physicalExpr)));

        costEstimatorResolver.resolve(s)
                .ifPresent(ce -> props.setCost(ce.getCost(physicalExpr)));

        p.setProperties(props);

        return p;
    }

    public Plan asPlan(TupleExpr physicalExpr) { return asPlan(physicalExpr, new PlanProperties()); }

    public Collection<Plan> enforceProps(Plan physicalExpr, RequestedPlanProperties requestedProps) {
        return enforceProps(Collections.singleton(physicalExpr), requestedProps);
    }

    public Collection<Plan> enforceProps(Collection<Plan> plans, RequestedPlanProperties requestedProps) {

        Stream<Plan> enforcedPlans = plans.stream();

        if (requestedProps.getDataProperties().isPresent()) {
            enforcedPlans = enforcedPlans
                    .flatMap( p ->
                            enforceDataProps(p, requestedProps.getDataProperties().get()).stream());
        }

        if (requestedProps.getSite().isPresent()) {
            enforcedPlans = enforcedPlans.flatMap( p ->
                    enforceSite(p, requestedProps.getSite().get()).stream());
        }

        return enforcedPlans.collect(Collectors.toList());
    }

    protected Collection<Plan> enforceDataProps(Plan plan, RequestedDataProperties dataProps) {
        if (!dataProps.isCoveredBy(plan.getProperties().getDataProperties()))
        {
            if (dataProps.ordering.isPresent()) {
                org.eclipse.rdf4j.query.algebra.Order o = new org.eclipse.rdf4j.query.algebra.Order(plan);
                Iterator<Ordering.OrderedVariable> vars = dataProps.ordering.get().getOrderedVariables();
                while (vars.hasNext()) {
                    Ordering.OrderedVariable v = vars.next();
                    boolean asc = v.getOrder() == Order.ASCENDING;
                    // FIXME: if v.getOrder() == ANY then produce all possible orders (i.e. ASC and DESC)
                    o.addElement(new OrderElem(new Var(v.getVariable()),asc));
                }

                return Collections.singleton(asPlan(o));
            }
        }
        return Collections.singleton(plan);
    }

    protected Collection<Plan> enforceSite(Plan plan, Site s) {
        if (!plan.getProperties().getSite().equals(s)) {
            // if the requested site is the local site then it is possible to enforce the property
            if (s.equals(LocalSite.getInstance())) {
                Plan pp = asPlan(new SourceQuery(plan, plan.getProperties().getSite()));
                return Collections.singleton(pp);
            } else {
                return Collections.emptyList();
            }
        }
        return Collections.singleton(plan);
    }

    /**
     * Decides whether a {@link Plan} {@code p1} is suboptimal and should be safely pruned.
     * The decision may consult a collection of equivalent plans.
     * @param p1 the {@link Plan} which is questioned whether to be pruned or not.
     * @param plans the collection of equivalent plans to help deciding.
     * @return True if the plan in question should be pruned; false otherwise.
     */
    public boolean canPrune(Plan p1, Collection<Plan> plans) {
        // check whether there is a plan in the collections that is superior of p and therefore p should be pruned
        return !shouldKeep(p1, plans);
    }

    public boolean shouldKeep(Plan p1, Collection<Plan> plans) {
        // we should keep plan p1 if it does not exist any plan p in plans such that
        // it can cover p1
        return !plans.stream().filter(pp -> !pp.equals(p1)).anyMatch( pp -> p1.getProperties().isCoveredBy(pp.getProperties()));
    }

    public void prune(Collection<Plan> plans) {

        Iterator<Plan> it = plans.iterator();

        while (it.hasNext()) {

            Plan p = it.next();

            if (this.canPrune(p, plans)) {
                it.remove();
            }
        }
    }

}
