package eu.semagrow.stack.modules.sails.semagrow.algebra;

import org.openrdf.model.URI;
import org.openrdf.query.algebra.QueryModelVisitor;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.UnaryTupleOperator;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by angel on 4/26/14.
 */
public class SourceQuery extends UnaryTupleOperator {

    List<URI> sources;
    // method to access sources (parallel, sequential) and with what order.
    // we choose parallel if we need the union (completeness of the result)
    // we choose sequential with the hope that some of the sources will not be needed eventually
    // (because of some LIMIT)

    public SourceQuery(TupleExpr expr) {
        super(expr);
    }

    public SourceQuery(TupleExpr expr, URI source) {
        super(expr);
        sources = new LinkedList<URI>();
        sources.add(source);
    }

    public SourceQuery(TupleExpr expr, List<URI> sources) {
        super(expr);
        this.sources = new LinkedList<URI>(sources);
    }

    public <X extends Exception> void visit(QueryModelVisitor<X> xQueryModelVisitor) throws X {
        xQueryModelVisitor.meetOther(this);
    }

    public String getSignature() {
        StringBuilder sb = new StringBuilder(128);

        sb.append(super.getSignature());
        for (URI src : sources) {
            sb.append(" (source = ");
            sb.append(src.toString());
            sb.append(")");
        }

        return sb.toString();
    }
}
