package eu.semagrow.core.impl.plan;

import eu.semagrow.core.impl.plan.ops.SourceQuery;
import eu.semagrow.core.estimator.CardinalityEstimator;
import eu.semagrow.core.impl.util.FilterCollector;
import eu.semagrow.core.plan.*;
import eu.semagrow.core.source.*;
import eu.semagrow.core.estimator.CostEstimator;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;
import org.eclipse.rdf4j.query.algebra.helpers.VarNameCollector;

import java.util.*;


/**
 * The default implementation of the @{link PlanGenerator}
 * @author Angelos Charalambidis
 */
public class PlanGeneratorImpl implements PlanGenerator, PlanGenerationContext {

    private SourceSelector sourceSelector;
    private CostEstimator        costEstimator;
    private CardinalityEstimator cardinalityEstimator;

    private DecomposerContext ctx;

    protected Collection<JoinImplGenerator> joinImplGenerators;

    public PlanGeneratorImpl(DecomposerContext ctx,
                             SourceSelector selector,
                             CostEstimator costEstimator,
                             CardinalityEstimator cardinalityEstimator)
    {
        this.ctx = ctx;
        this.sourceSelector = selector;
        this.costEstimator = costEstimator;
        this.cardinalityEstimator = cardinalityEstimator;

        this.joinImplGenerators = new LinkedList<JoinImplGenerator>();
        this.joinImplGenerators.add(new BindJoinGenerator());
        this.joinImplGenerators.add(new RemoteJoinGenerator());
        //this.joinImplGenerators.add(new HashJoinGenerator());
        //this.joinImplGenerators.add(new MergeJoinGenerator());
    }


    @Override
    public Collection<Plan> accessPlans(TupleExpr expr, BindingSet bindings, Dataset dataset)
    {

        Collection<Plan> plans = new LinkedList<Plan>();


        List<BindingSetAssignment> assignments = BindingSetAssignmentCollector.process(expr);

        for (BindingSetAssignment a : assignments) {
            Set<TupleExpr> exprLabel =  new HashSet<TupleExpr>();
            exprLabel.add(a);
            Plan p = create(exprLabel, a);
            plans.add(p);
        }


        // extract the statement patterns
        List<StatementPattern> statementPatterns = StatementPatternCollector.process(expr);

        // extract the filter conditions of the query

        for (StatementPattern pattern : statementPatterns) {

            // get sources for each pattern
            Collection<SourceMetadata> sources = getSources(pattern, dataset, bindings);

            // apply filters that can be applied to the statementpattern
            TupleExpr e = pattern;

            Set<TupleExpr> exprLabel =  new HashSet<TupleExpr>();
            exprLabel.add(e);

            List<Plan> sourcePlans = new LinkedList<Plan>();

            if (sources.isEmpty()) {
                plans.add(create(exprLabel, new EmptySet()));

            } else {

                for (SourceMetadata sourceMetadata : sources) {
                    //URI source = sourceMetadata.getSites().get(0);
                    //Plan p1 = createPlan(exprLabel, sourceMetadata.target(), source, ctx);
                    // FIXME: Don't use always the first source.
                    Plan p1 = createPlan(exprLabel, sourceMetadata.target().clone(), sourceMetadata);
                    sourcePlans.add(p1);
                }

                Plan p = createUnionPlan(sourcePlans);
                plans.add(p);
            }
        }

        return plans;
    }

    public Plan createUnionPlan(List<Plan> plans)
    {
        Iterator<Plan> it = plans.iterator();
        Plan p;

        if (it.hasNext())
            p = it.next();
        else
            return null;

        while (it.hasNext()) {
            Plan i = it.next();
            p = create(p.getKey(), new Union(enforce(p, LocalSite.getInstance()), enforce(i, LocalSite.getInstance())));
        }

        return p;
    }

    protected Collection<SourceMetadata> getSources(StatementPattern pattern, Dataset dataset, BindingSet bindings) {
        return sourceSelector.getSources(pattern,dataset,bindings);
    }

    @Override
    public Collection<Plan> joinPlans(Collection<Plan> plan1, Collection<Plan> plan2)
    {
        Collection<Plan> plans = new LinkedList<Plan>();

        for (JoinImplGenerator generator : joinImplGenerators) {

            for (Plan p1 : plan1) {
                for (Plan p2 : plan2) {
                    Set<TupleExpr> s = getKey(p1.getKey(), p2.getKey());

                    Collection<Join> joins = generator.generate(p1, p2, this);

                    for (Join j : joins)
                        plans.add(create(s, j));
                }
            }
        }
        return plans;
    }

    private Set<TupleExpr> getKey(Set<TupleExpr> id1, Set<TupleExpr> id2) {
        Set<TupleExpr> s = new HashSet<TupleExpr>(id1);
        s.addAll(id2);
        return s;
    }

    @Override
    public Collection<Plan> finalizePlans(Collection<Plan> plans, PlanProperties properties) {
        Collection<Plan> pl = new LinkedList<Plan>();

        for (Plan p : plans)
            pl.add(enforce(p, LocalSite.getInstance()));

        return pl;
    }

    @Override
    public Plan enforce(Plan p, Site site) {
        Site s1 = p.getProperties().getSite();
        if (s1.isLocal()) {
            return p;
        }
        else {
            ///// FIXME
            TupleExpr expr = s1.getCapabilities().enforceSite(p);
            if (expr instanceof EmptySet) {
                return create(p.getKey(), new EmptySet());
            }
            if (expr instanceof Filter) {
                TupleExpr newexpr = new Filter(
                        new SourceQuery(((Filter) expr).getArg(), s1),
                        ((Filter) expr).getCondition()
                );
                return create(p.getKey(), newexpr);
            }
            else {
                return create(p.getKey(), new SourceQuery(expr, s1));
            }
            //return create(p.getKey(), new SourceQuery(p, s1));
        }
    }

    @Override
    public Plan enforce(Plan p, Ordering ordering) {
        if (p.getProperties().getOrdering().isCoverOf(ordering)) {
            return p;
        } else {
            return create(p.getKey(), new Order(p, ordering.getOrderElements()));
        }
    }

    public Plan create(Set<TupleExpr> id, TupleExpr e) {

        /*
        TupleExpr e1 = PlanUtil.applyRemainingFilters(e.clone(), ctx.getFilters());

        Plan p = Plan.create(id, e1);

        p.getProperties().setCost(costEstimator.getCost(e, p.getProperties().getSite()));
        p.getProperties().setCardinality(cardinalityEstimator.getCardinality(e, p.getProperties().getSite().getURI()));
        */
        return createPlan(id, e);
        //return p;
    }


    protected void updatePlanProperties(Plan plan)
    {
        TupleExpr e = plan.getArg();

        PlanProperties properties = PlanPropertiesUpdater.process(e, plan.getProperties());

        plan.setProperties(properties);

        // update cardinality and cost properties
        plan.getProperties().setCost(costEstimator.getCost(e, plan.getProperties().getSite()));
        plan.getProperties().setCardinality(cardinalityEstimator.getCardinality(e, plan.getProperties().getSite()));

    }

    /**
     * Update the properties of a plan
     * @param plan
     */
    protected Plan updatePlan(Plan plan)
    {
        //TupleExpr innerExpr = plan.getArg();

        updatePlanProperties(plan);

        // apply filters that can be applied
        Plan e = applyRemainingFilters(plan, ctx.getFilters());

        updatePlanProperties(e);

        return e;
    }

    public Plan applyRemainingFilters(Plan e, Collection<ValueExpr> conditions) {
        Collection<ValueExpr> filtersApplied = FilterCollector.process(e);
        Collection<ValueExpr> remainingFilters = getRelevantFiltersConditions(e, conditions);
        remainingFilters.removeAll(filtersApplied);

        SourceCapabilities srcCap = e.getProperties().getSite().getCapabilities();

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

    public Plan applyFilters(Plan e, Collection<ValueExpr> conditions) {

        if (conditions.isEmpty()) {
            return e;
        }
        TupleExpr expr = e.clone();

        for (ValueExpr condition : conditions)
            expr = new Filter(expr, condition);

        return new Plan(e.getKey(), expr);
        //return createPlan(e.getKey(), expr);
    }

    protected Plan createPlan(Set<TupleExpr> planId, TupleExpr innerExpr)
    {
        Plan p = new Plan(planId, innerExpr);
        //updatePlanProperties(p);
        return updatePlan(p);
    }

    protected Plan createPlan(Set<TupleExpr> planId, TupleExpr innerExpr, SourceMetadata metadata)
    {
        Site site = metadata.getSites().get(0);

        Plan p = new Plan(planId, innerExpr);

        Set<String> varNames = innerExpr.getBindingNames();

        /*
        for (String varName : varNames) {
            Collection<URI> schemas = metadata.getSchema(varName);
            if (!schemas.isEmpty())
                p.setSchemas(varName, schemas);
        }
        */
        p.getProperties().setSite(site);
        updatePlan(p);

        return p;
    }
}
