package org.semagrow.plan.util;

import org.eclipse.rdf4j.query.algebra.BindingSetAssignment;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by angel on 2/11/2015.
 */
public class BindingSetAssignmentCollector extends AbstractQueryModelVisitor<RuntimeException> {

    private List<BindingSetAssignment> stPatterns = new ArrayList<>();

    public BindingSetAssignmentCollector() {
    }

    public static List<BindingSetAssignment> process(QueryModelNode node) {
        BindingSetAssignmentCollector collector = new BindingSetAssignmentCollector();
        node.visit(collector);
        return collector.getBindingSetAssigments();
    }

    public List<BindingSetAssignment> getBindingSetAssigments() {
        return this.stPatterns;
    }

    public void meet(Filter node) {
        node.getArg().visit(this);
    }

    public void meet(BindingSetAssignment node) {
        this.stPatterns.add(node);
    }
}
