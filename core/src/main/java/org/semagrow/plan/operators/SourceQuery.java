package org.semagrow.plan.operators;

import org.semagrow.selector.Site;
import org.eclipse.rdf4j.query.algebra.QueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.UnaryTupleOperator;

/**
 * Source query represents the operation of executing its argument using
 * an appropriate {@link org.semagrow.evaluation.QueryExecutor} to a
 * specific site and fetch the result of the execution to the local site.
 *
 * @see org.semagrow.evaluation.QueryExecutor
 * @see org.semagrow.evaluation.QueryExecutorResolver
 * @see org.semagrow.local.LocalSite
 * @author acharal
 */
public class SourceQuery extends UnaryTupleOperator {

    private Site site;

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
