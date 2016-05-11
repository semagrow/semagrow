package eu.semagrow.core.impl.plan;

import org.openrdf.query.algebra.BindingSetAssignment;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by angel on 2/11/2015.
 */
public class BindingSetAssignmentCollector extends QueryModelVisitorBase<RuntimeException> {

    private List<BindingSetAssignment> stPatterns = new ArrayList();

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
