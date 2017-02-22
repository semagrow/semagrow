package org.semagrow.plan.queryblock;

import com.google.common.collect.Sets;
import org.eclipse.rdf4j.query.algebra.AggregateOperator;
import org.eclipse.rdf4j.query.algebra.Group;
import org.eclipse.rdf4j.query.algebra.GroupElem;
import org.semagrow.plan.CompilerContext;
import org.semagrow.plan.Plan;
import org.semagrow.plan.RequestedDataProperties;
import org.semagrow.plan.RequestedPlanProperties;
import org.semagrow.plan.operators.StreamGroup;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author acharal
 */
public class GroupBlock extends AbstractQueryBlock {

    private Set<String> groupByVariables;

    private Map<String, AggregateOperator> aggregations;

    private QueryBlock sourceBlock;

    public GroupBlock(QueryBlock arg, Collection<String> groups) {
        assert arg != null;
        this.sourceBlock = arg;

        this.groupByVariables = new HashSet<>();
        this.aggregations = new HashMap<>();
        this.groupByVariables.addAll(groups);

        setDuplicateStrategy(OutputStrategy.ENFORCE);
    }

    public void addAggregation(String val, AggregateOperator op){
        aggregations.put(val, op);
    }

    public Set<String> getOutputVariables() {
        return Sets.union(groupByVariables, aggregations.keySet());
    }

    public Set<String> getGroupedVariables() { return groupByVariables; }

    @Override
    public <X extends Exception> void visitChildren(QueryBlockVisitor<X> visitor) throws X {
        sourceBlock.visit(visitor);
    }

    public boolean hasDuplicates() { return false; }

    public Collection<Plan> getPlans(CompilerContext context) {
        Collection<Plan> plans = sourceBlock.getPlans(context);
        return plans.stream().flatMap(p -> getGroupPlan(context, p))
                .collect(Collectors.toList());
    }

    private Stream<Plan> getGroupPlan(CompilerContext context, Plan p) {

        Collection<GroupElem> groupElems = this.aggregations.entrySet().stream()
                .map(entry -> new GroupElem(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());

        Stream<Plan> stream1 = Stream.of(context.asPlan(new Group(p, getGroupedVariables(), groupElems)));

        RequestedPlanProperties props = new RequestedPlanProperties();
        props.setDataProperties(RequestedDataProperties.forGrouping(getGroupedVariables()));
        Collection<Plan> p1 = context.enforceProps(p, props);
        Stream<Plan> stream2 = p1.stream().flatMap(p2 ->
                Stream.of(context.asPlan(new StreamGroup(p2, getGroupedVariables(), groupElems))));

        return Stream.concat(stream1, stream2);
    }
}
