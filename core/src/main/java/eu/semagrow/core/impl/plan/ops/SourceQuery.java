package eu.semagrow.core.impl.plan.ops;

import eu.semagrow.core.source.Site;
import org.openrdf.query.algebra.QueryModelVisitor;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.UnaryTupleOperator;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by angel on 4/26/14.
 */
public class SourceQuery extends UnaryTupleOperator {

    List<Site> sources;
    // method to access sources (parallel, sequential) and with what order.
    // we choose parallel if we need the union (completeness of the result)
    // we choose sequential with the hope that some of the sources will not be needed eventually
    // (because of some LIMIT)

    public SourceQuery(TupleExpr expr) {
        super(expr);
    }

    public SourceQuery(TupleExpr expr, Site fromSite) {
        super(expr);
        sources = new LinkedList<Site>();
        sources.add(fromSite);
    }

    public SourceQuery(TupleExpr expr, List<Site> sites) {
        super(expr);
        this.sources = new LinkedList<Site>(sites);
    }

    public List<Site> getSources() { return sources; }

    public <X extends Exception> void visit(QueryModelVisitor<X> xQueryModelVisitor) throws X {
        xQueryModelVisitor.meetOther(this);
    }

    public String getSignature() {
        StringBuilder sb = new StringBuilder(128);

        sb.append(super.getSignature());
        for (Site src : sources) {
            sb.append(" (source = ");
            sb.append(src.toString());
            sb.append(")");
        }

        return sb.toString();
    }

    @Override
    public int hashCode() {

        return this.sources.hashCode() + super.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof SourceQuery) {
            return this.sources.equals(((SourceQuery) o).sources) &&
                   super.equals(o);
        }
        return false;
    }
}
