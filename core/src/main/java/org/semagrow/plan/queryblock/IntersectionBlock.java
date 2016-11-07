package org.semagrow.plan.queryblock;

import org.eclipse.rdf4j.query.algebra.Intersection;
import org.semagrow.plan.CompilerContext;
import org.semagrow.plan.Plan;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by angel on 7/9/2016.
 */
public class IntersectionBlock extends AbstractQueryBlock {

    private Collection<QueryBlock> blocks;

    public IntersectionBlock(QueryBlock...block) {
        List<QueryBlock> blocks = Arrays.asList(block);
        this.blocks = new ArrayList<>(blocks);
    }

    public Set<String> getOutputVariables() {

        return blocks.stream()
                .flatMap(b -> b.getOutputVariables().stream())
                .distinct()
                .collect(Collectors.toSet());
    }

    public void add(QueryBlock b) { blocks.add(b); }

    public void addAll(Collection<QueryBlock> bs) { blocks.addAll(bs); }

    @Override
    public <X extends Exception> void visitChildren(QueryBlockVisitor<X> visitor) throws X {
        for (QueryBlock b : blocks)
            b.visit(visitor);
    }

    public boolean hasDuplicates() {
        if (getDuplicateStrategy() == OutputStrategy.PRESERVE) {
            // if intersection preserves the duplicates then
            // the only case that there are no duplicates is when
            // all its constituent blocks do not have duplicates.
            return !blocks.stream().noneMatch(QueryBlock::hasDuplicates);
        } else {
            return true;
        }
    }

    public Collection<Plan> getPlans(CompilerContext context) {

        Stream<Collection<Plan>> plans = blocks.stream().map(b -> b.getPlans(context));

        //FIXME: Check the Site property
        Collection<Plan> planAlternatives = plans.reduce((p1, p2) -> getIntersectionPlan(context,p1,p2))
                .orElse(Collections.emptyList());

        //FIXME: prune if necessary
        // prune plan Alternatives
        return planAlternatives;
    }

    public Collection<Plan> getIntersectionPlan(CompilerContext context, Collection<Plan> p1, Collection<Plan> p2) {
        return p1.stream().flatMap( pp1 ->
                p2.stream().flatMap(pp2 ->
                        Stream.of(context.asPlan(new Intersection(pp1, pp2)))
                )
        ).collect(Collectors.toList());
    }

}
