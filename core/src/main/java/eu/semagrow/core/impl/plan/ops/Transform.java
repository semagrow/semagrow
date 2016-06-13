package eu.semagrow.core.impl.plan.ops;

import org.eclipse.rdf4j.query.algebra.QueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.UnaryTupleOperator;

/**
 * Created by angel on 5/29/14.
 */
public class Transform extends UnaryTupleOperator {

    public <X extends Exception> void visit(QueryModelVisitor<X> xQueryModelVisitor) throws X {
        xQueryModelVisitor.meetOther(this);
    }

}
