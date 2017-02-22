package org.semagrow.plan;

import org.semagrow.plan.queryblock.*;

import java.util.Collection;

/**
 * Traverses the QueryBlock AST infers {@link InterestingProperties}
 * and associates them to each {@link QueryBlock}
 * @author acharal
 * @since 2.0
 */
public class InterestingPropertiesVisitor extends AbstractQueryBlockVisitor<RuntimeException> {

    private InterestingProperties intProps = new InterestingProperties();

    @Override
    public void meet(SelectBlock b) {

        InterestingProperties props = coverWithOutput(b, intProps);

        b.setInterestingProperties(props);

        InterestingProperties inputProps = props.clone();

        if (b.getDuplicateStrategy() == OutputStrategy.ENFORCE) {
            inputProps.addStructureProperties(RequestedDataProperties.forGrouping(b.getOutputVariables()));
        }

        for (Quantifier q : b.getQuantifiers()) {
            intProps = homogenize(inputProps, q.getBlock().getOutputVariables());

            //TODO: determine interesting orderings arising from merge-joins

            q.getBlock().visit(this);
        }
    }

    @Override
    public void meet(GroupBlock b) {

        InterestingProperties props = coverWithOutput(b, intProps);

        b.setInterestingProperties(props);

        InterestingProperties inputProps = props.clone();
        inputProps.addStructureProperties(RequestedDataProperties.forGrouping(b.getGroupedVariables()));

        // no homogenization since groupblock does not change the name of the variables
        //intProps = homogenize(inputProps, b.getOutputVariables());

        super.meet(b);
    }

    @Override
    public void meet(UnionBlock b) {
        b.setInterestingProperties(intProps);
        super.meet(b);
    }

    @Override
    public void meet(IntersectionBlock b) {
        b.setInterestingProperties(intProps);
        super.meet(b);
    }

    @Override
    public void meet(MinusBlock b) {
        b.setInterestingProperties(intProps);
        super.meet(b);
    }

    @Override
    public void meet(PatternBlock b) {
        b.setInterestingProperties(intProps);
    }

    @Override
    public void meet(BindingSetAssignmentBlock b) {
        b.setInterestingProperties(intProps);
    }

    protected InterestingProperties coverWithOutput(QueryBlock b, InterestingProperties intProps) {

        InterestingProperties props = new InterestingProperties();

        // output requirements are the properties that will be forced by the block anyway
        DataProperties outputReqs = b.getOutputDataProperties();

        // therefore they are ``interesting'' in any case.
        props.addStructureProperties(outputReqs.asRequestedProperties());

        // check if there are any inherited interesting properties that are covered by the output requirements
        // if so, then there is no need to include them, since the operator will satisfy them by default

        for (RequestedDataProperties reqProps : intProps.getStructureProperties()) {
            if (!reqProps.isCoveredBy(outputReqs)) {
                props.addStructureProperties(reqProps);
            }
        }

        // assure the set does not contain any trivial requested properties
        props.dropTrivials();

        return props;
    }

    protected InterestingProperties homogenize(InterestingProperties props, Collection<String> variables) {
        // try to substitute each column in props with a column in variables (based on the equivalent classes)
        return props;
    }

}