package org.semagrow.plan;

import org.semagrow.local.LocalSite;
import org.semagrow.plan.operators.SourceQuery;
import org.semagrow.plan.util.FilterCollector;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.helpers.VarNameCollector;
import org.semagrow.selector.*;

import java.util.*;


/**
 * The default implementation of the {@link PlanGenerator}
 * @author acharal
 */
public class SimplePlanGenerator implements PlanGenerator, PlanGenerationContext {

    private SourceSelector sourceSelector;
    private PlanFactory planFactory;

    private DecomposerContext ctx;

    protected Collection<JoinImplGenerator> joinImplGenerators;

    public SimplePlanGenerator(DecomposerContext ctx,
                               SourceSelector selector,
                               PlanFactory planFactory)
    {
        this.ctx = ctx;
        this.sourceSelector = selector;
        this.planFactory = planFactory;

        this.joinImplGenerators = new LinkedList<JoinImplGenerator>();
        this.joinImplGenerators.add(new BindJoinGenerator());
        this.joinImplGenerators.add(new RemoteJoinGenerator());
        //this.joinImplGenerators.add(new HashJoinGenerator());
        //this.joinImplGenerators.add(new MergeJoinGenerator());
    }


    @Override
    public PlanCollection accessPlans(TupleExpr expr, BindingSet bindings, Dataset dataset)
    {
        PlanCollection plans = new PlanCollectionImpl(expr);

        if (expr instanceof BindingSetAssignment)
        {
            plans.add(create(expr));
            return plans;
        }

        // get sources for each pattern
        Collection<SourceMetadata> sources = getSources(expr, dataset, bindings);

        List<Plan> sourcePlans = new LinkedList<Plan>();

        if (sources.isEmpty()) {
            plans.add(create(new EmptySet()));

        } else {

            for (SourceMetadata sourceMetadata : sources) {
                //URI source = sourceMetadata.getSites().get(0);
                //Plan p1 = createPlan(exprLabel, sourceMetadata.target(), source, ctx);
                // FIXME: Don't use always the first source.
                Plan p1 = createPlan(sourceMetadata.target().clone(), sourceMetadata);
                sourcePlans.add(p1);
            }

            Plan p = createUnionPlan(sourcePlans);
            plans.add(p);
        }

        return plans;
    }

    protected Collection<SourceMetadata> getSources(TupleExpr pattern, Dataset dataset, BindingSet bindings) {
        return sourceSelector.getSources(pattern,dataset,bindings);
    }

    @Override
    public PlanCollection joinPlans(PlanCollection plan1, PlanCollection plan2)
    {
        Set<TupleExpr> l = new HashSet<>(plan1.getLogicalExpr());
        l.addAll(plan2.getLogicalExpr());

        return joinImplGenerators.stream()
                .flatMap(gen ->
                        plan1.stream()
                                .flatMap( p1 ->
                                        plan2.stream().flatMap(p2 ->
                                        gen.generate(p1,p2,this).stream()
                                                    .map(j -> create(j)))))
                .collect(PlanCollectionImpl.toPlanCollection(l));
    }

    public Plan createUnionPlan(List<Plan> plans)
    {
        Site s = LocalSite.getInstance();
        Optional<Plan> unionedPlan = plans.stream()
                .reduce( (p1,p2)-> create(new Union(enforce(p1,s), enforce(p2,s))));

        if (unionedPlan.isPresent())
            return unionedPlan.get();
        else
            throw new AssertionError("the list of plans is empty in createUnionPlan");
    }


    @Override
    public PlanCollection finalizePlans(PlanCollection plans, PlanProperties properties)
    {
        return plans.stream()
                .map(p -> enforce(p, LocalSite.getInstance()))
                .collect(PlanCollectionImpl.toPlanCollection(plans.getLogicalExpr()));
    }

    @Override
    public Plan enforce(Plan p, Site site) {
        Site s1 = p.getProperties().getSite();
        if (s1.isRemote()) {
            ///// FIXME
            TupleExpr expr = s1.getCapabilities().enforceSite(p);
            if (expr instanceof EmptySet) {
                return create(new EmptySet());
            }
            if (expr instanceof Filter) {
                TupleExpr newexpr = new Filter(
                        new SourceQuery(((Filter) expr).getArg(), s1),
                        ((Filter) expr).getCondition()
                );
                return create(newexpr);
            }
            else {
                return create(new SourceQuery(expr, s1));
            }
            //return create(p.getKey(), new SourceQuery(p, s1));
        } else {
            return p;
        }
    }

    @Override
    public Plan enforce(Plan p, Ordering ordering) {
        /*
        if (p.getProperties().getOrdering().isCoverOf(ordering)) {
            return p;
        } else {
            return create(new Order(p, ordering.getOrderElements()));
        }
        */
        return p;
    }

    public TupleExpr applyRemainingFilters(Plan e, Collection<ValueExpr> conditions) {
        Collection<ValueExpr> filtersApplied = FilterCollector.process(e);
        Collection<ValueExpr> remainingFilters = getRelevantFiltersConditions(e, conditions);
        remainingFilters.removeAll(filtersApplied);

        SiteCapabilities srcCap = e.getProperties().getSite().getCapabilities();

        Collection<ValueExpr> legitFilters = new LinkedList<>();

        for (ValueExpr cond : remainingFilters) {
            if (srcCap.acceptsFilter(e, cond))
                legitFilters.add(cond);
        }

        return applyFilters(e, legitFilters);
    }

    public Collection<ValueExpr> getRelevantFiltersConditions(Plan e, Collection<ValueExpr> filterConditions)
    {
        Set<String> variables = VarNameCollector.process(e);
        Collection<ValueExpr> relevantConditions = new LinkedList<ValueExpr>();

        for (ValueExpr condition : filterConditions) {
            Set<String> conditionVariables = VarNameCollector.process(condition);
            if (variables.containsAll(conditionVariables))
                relevantConditions.add(condition);
        }

        return relevantConditions;
    }

    public TupleExpr applyFilters(Plan e, Collection<ValueExpr> conditions) {

        if (conditions.isEmpty()) {
            return e;
        }
        TupleExpr expr = e.clone();

        for (ValueExpr condition : conditions)
            expr = new Filter(expr, condition);

        return expr;
    }


    public Plan create(TupleExpr e) {

        Plan p = planFactory.create(e);
        e = applyRemainingFilters(p, ctx.getFilters());
        p = planFactory.create(e);
        return p;

    }

    protected Plan createPlan(TupleExpr innerExpr, SourceMetadata metadata)
    {
        Site site = metadata.getSites().iterator().next();

        PlanProperties prop = PlanProperties.defaultProperties();
        prop.setSite(site);

        return planFactory.create(innerExpr, prop);
    }
}
