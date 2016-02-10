package eu.semagrow.commons.algebra;

import org.openrdf.query.algebra.QueryModelVisitor;
import org.openrdf.query.algebra.UnaryTupleOperator;

/**
 * Created by angel on 5/29/14.
 */
public class Transform extends UnaryTupleOperator {

    public <X extends Exception> void visit(QueryModelVisitor<X> xQueryModelVisitor) throws X {
        xQueryModelVisitor.meetOther(this);
    }

}
