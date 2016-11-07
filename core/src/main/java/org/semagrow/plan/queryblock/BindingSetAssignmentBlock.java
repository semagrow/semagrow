package org.semagrow.plan.queryblock;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.BindingSetAssignment;
import org.semagrow.plan.CompilerContext;
import org.semagrow.plan.Plan;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

/**
 *
 * @author acharal
 */
public class BindingSetAssignmentBlock extends AbstractQueryBlock {


    private Set<String> bindingNames;

    private Iterable<BindingSet> bindingSets;

    public BindingSetAssignmentBlock(Set<String> bindingNames, Iterable<BindingSet> bindingSets) {
        assert bindingNames != null && bindingSets != null;
        this.bindingNames = bindingNames;
        this.bindingSets = bindingSets;
    }

    @Override
    public Set<String> getOutputVariables() { return bindingNames; }

    public boolean hasDuplicates() { return true; }

    public Collection<Plan> getPlans(CompilerContext context) {
        BindingSetAssignment expr = new BindingSetAssignment();
        expr.setBindingNames(bindingNames);
        expr.setBindingSets(bindingSets);
        return Collections.singleton(context.asPlan(expr));
    }

}
