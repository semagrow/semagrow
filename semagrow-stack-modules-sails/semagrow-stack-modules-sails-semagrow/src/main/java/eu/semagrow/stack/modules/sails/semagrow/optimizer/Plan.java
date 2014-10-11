package eu.semagrow.stack.modules.sails.semagrow.optimizer;

import org.openrdf.model.URI;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.algebra.QueryModelVisitor;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.UnaryTupleOperator;

import java.util.Set;

/**
 * Created by angel on 9/30/14.
 */
public class Plan extends UnaryTupleOperator {

    static public URI LOCAL = ValueFactoryImpl.getInstance().createURI("http://www.semagrow.eu/");

    // expected cardinality
    private long card;

    // expected cost
    private double cost;

    private Set<TupleExpr> id;

    private Ordering ordering;

    private URI site;

    public Plan(TupleExpr arg) {
        super(arg);
    }

    public Plan(Set<TupleExpr> id, TupleExpr arg) {
        super(arg);
        this.id = id;
        this.ordering = Ordering.NoOrdering();
        this.site = LOCAL;
    }

    public Set<TupleExpr> getPlanId() {
        return this.id;
    }

    public long getCardinality() { return card; }

    public void setCardinality(long card) { this.card = card;}

    public double getCost() { return cost; }

    public void setCost(double cost) { this.cost = cost; }

    public URI getSite() { return site; }

    public void setSite(URI site) { this.site = site; }

    public Ordering getOrdering() { return ordering; }

    public void setOrdering(Ordering ordering) { this.ordering = ordering; }

    public Iterable<Object> getProperties() { return null; }

    public PlanCollection getSubplans() { return null; }

    public <X extends Exception> void visit(QueryModelVisitor<X> xQueryModelVisitor) throws X {
        //getArg().visit(xQueryModelVisitor);
        xQueryModelVisitor.meetOther(this);
    }

    public String getSignature() {
        StringBuilder sb = new StringBuilder(128);

        sb.append(super.getSignature());

        sb.append(" (cost = ");
        sb.append(cost);
        sb.append(", card = ");
        sb.append(card);
        sb.append(", source = ");
        sb.append(site.toString());
        sb.append(")");

        return sb.toString();
    }
}
