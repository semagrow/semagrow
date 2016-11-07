package org.semagrow.plan.queryblock;

/**
 * Created by angel on 13/9/2016.
 */
public class AbstractQueryBlockVisitor<X extends Exception> implements QueryBlockVisitor<X> {


    @Override
    public void meet(QueryBlock b) throws X {
        if (b instanceof SelectBlock)
            meet((SelectBlock)b);
        else if (b instanceof GroupBlock)
            meet((GroupBlock)b);
        else if (b instanceof UnionBlock)
            meet((UnionBlock)b);
        else if (b instanceof IntersectionBlock)
            meet((IntersectionBlock)b);
        else if (b instanceof MinusBlock)
            meet((MinusBlock)b);
        else if (b instanceof PatternBlock)
            meet((PatternBlock)b);
        else if (b instanceof BindingSetAssignmentBlock)
            meet((BindingSetAssignmentBlock)b);
        else
            meetOther(b);
    }

    public void meet(SelectBlock b) throws X { meetNode(b); }

    public void meet(GroupBlock b) throws X { meetNode(b); }

    public void meet(UnionBlock b) throws X { meetNode(b); }

    public void meet(IntersectionBlock b) throws X { meetNode(b); }

    public void meet(MinusBlock b) throws X { meetNode(b); }

    public void meet(PatternBlock b) throws X { meetNode(b); }

    public void meet(BindingSetAssignmentBlock b) throws X { meetNode(b); }

    public void meetOther(QueryBlock b) throws X { meetNode(b); }

    public void meetNode(QueryBlock b) throws X { b.visitChildren(this); }

}
