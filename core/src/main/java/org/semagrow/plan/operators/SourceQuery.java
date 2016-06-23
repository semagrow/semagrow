package org.semagrow.plan.operators;

import org.semagrow.selector.Site;
import org.eclipse.rdf4j.query.algebra.QueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.UnaryTupleOperator;


/**
 * Created by angel on 4/26/14.
 */
public class SourceQuery extends UnaryTupleOperator {

    Site site;
    // method to access sources (parallel, sequential) and with what order.
    // we choose parallel if we need the union (completeness of the result)
    // we choose sequential with the hope that some of the sources will not be needed eventually
    // (because of some LIMIT)

    public SourceQuery(TupleExpr expr) {
        super(expr);
    }

    public SourceQuery(TupleExpr expr, Site fromSite) {
        super(expr);
        site = fromSite;
    }

    public Site getSite() { return site; }

    public <X extends Exception> void visit(QueryModelVisitor<X> xQueryModelVisitor) throws X {
        xQueryModelVisitor.meetOther(this);
    }

    public String getSignature() {
        StringBuilder sb = new StringBuilder(128);

        sb.append(super.getSignature());
        sb.append(" (source = ");
        sb.append(site.toString());
        sb.append(")");

        return sb.toString();
    }

    @Override
    public int hashCode() {

        return this.site.hashCode() + super.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof SourceQuery) {
            return this.site.equals(((SourceQuery) o).site) &&
                   super.equals(o);
        }
        return false;
    }
}
