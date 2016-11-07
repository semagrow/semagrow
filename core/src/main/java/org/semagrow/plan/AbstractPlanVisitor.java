package org.semagrow.plan;

import org.semagrow.plan.operators.*;
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

/**
 * An abstract convenient default implementation of a {@link PlanVisitor}.
 * Every <tt>meet(... node)</tt> calls the {@link #meetNode(QueryModelNode)}
 * that eventually visits the children of the node.
 * @author acharal
 */
public abstract class AbstractPlanVisitor<X extends Exception>
        extends AbstractQueryModelVisitor<X>
        implements PlanVisitor<X>  {

    public AbstractPlanVisitor() {

    }

    public void meet(Plan plan) throws X {
        meetPlan(plan);
    }

    public void meet(HashJoin join) throws X {
        meet((Join) join);
    }

    public void meet(BindJoin join) throws X {
        meet((Join) join);
    }

    public void meet(MergeJoin join) throws X {
        meet((Join) join);
    }

    public void meet(MergeUnion union) throws X {
        meet((Union) union);
    }

    public void meet(SourceQuery query) throws X {
        meetNode(query);
    }

    protected void meetPlan(Plan plan) throws X {
        meetNode(plan);
    }

    @Override
    public void meetOther(QueryModelNode node) throws X {

        if (node instanceof Plan)
            meet((Plan)node);
        else if (node instanceof SourceQuery)
            meet((SourceQuery)node);
        else if (node instanceof BindJoin)
            meet((BindJoin)node);
        else if (node instanceof HashJoin)
            meet((HashJoin)node);
        else if (node instanceof MergeJoin)
            meet((MergeJoin)node);
        else if (node instanceof MergeUnion)
            meet((MergeUnion)node);
        else
            super.meetOther(node);
    }


}
