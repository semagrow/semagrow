package eu.semagrow.stack.modules.sails.semagrow.algebra;

import org.openrdf.model.URI;
import org.openrdf.query.algebra.QueryModelVisitor;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.UnaryTupleOperator;

/**
 * Created by angel on 3/14/14.
 */
@Deprecated
public class SingleSourceExpr extends UnaryTupleOperator {

    private URI source;

    public SingleSourceExpr(TupleExpr expr, URI source) {
        super(expr);
        setSource(source);
    }

    public URI getSource() {
        return source;
    }

    public void setSource(URI source) {
        this.source = source;
    }

    public <X extends Exception> void visit(QueryModelVisitor<X> visitor) throws X {
        visitor.meetOther(this);
    }

    public String getSignature() {
        StringBuilder sb = new StringBuilder(128);

        sb.append(super.getSignature());
        sb.append(" (source = ");
        sb.append(getSource().toString());
        sb.append(")");

        return sb.toString();
    }
}
