package org.semagrow.plan.queryblock;


import org.eclipse.rdf4j.query.algebra.Union;
import org.semagrow.local.LocalSite;
import org.semagrow.plan.CompilerContext;
import org.semagrow.plan.Plan;
import org.semagrow.plan.RequestedPlanProperties;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by angel on 6/9/2016.
 */
public class UnionBlock extends AbstractQueryBlock {

    private Collection<QueryBlock> blocks;

    public UnionBlock(QueryBlock...block) {
        List<QueryBlock> blocks = Arrays.asList(block);
        this.blocks = new ArrayList<>(blocks);
    }

    public UnionBlock(Collection<QueryBlock> blocks) {
        this();
        this.addAll(blocks);
    }

    @Override
    public Set<String> getOutputVariables() {
        return blocks.stream()
                .flatMap(b -> b.getOutputVariables().stream())
                .distinct()
                .collect(Collectors.toSet());
    }

    @Override
    public <X extends Exception> void visitChildren(QueryBlockVisitor<X> visitor) throws X {
        for (QueryBlock b : blocks)
            visitor.meet(b);
    }

    public Collection<QueryBlock> getBlocks() { return blocks; }

    public void add(QueryBlock b) { blocks.add(b); }

    public void addAll(Collection<QueryBlock> bs) { blocks.addAll(bs); }

    public boolean hasDuplicates() { return true; }


    public Collection<Plan> getPlans(CompilerContext context) {

        Stream<Collection<Plan>> plans = blocks.stream().map(b -> b.getPlans(context));

        //FIXME: Check the Site property
        Collection<Plan> planAlternatives = plans.reduce((p1, p2) -> getUnionPlan(context,p1,p2))
                .orElse(Collections.emptyList());

        //FIXME: prune if necessary
        return planAlternatives;
    }

    public Collection<Plan> getUnionPlan(CompilerContext context, Collection<Plan> p1, Collection<Plan> p2) {

        if (p1.isEmpty())
            return p2;

        if (p2.isEmpty())
            return p1;

        //FIXME: try also ordered union merge
        //FIXME: check that pp1 and pp2 is in the same site or need shipping

        RequestedPlanProperties props = new RequestedPlanProperties();
        props.setSite(LocalSite.getInstance());

        return context.enforceProps(p1, props).stream().flatMap(pp1 ->
                context.enforceProps(p2, props).stream().flatMap(pp2 ->
                        Stream.of(context.asPlan(new Union(pp1, pp2)))
                )
        ).collect(Collectors.toList());
    }
}
