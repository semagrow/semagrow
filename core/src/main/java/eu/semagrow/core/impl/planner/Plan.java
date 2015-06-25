package eu.semagrow.core.impl.planner;

import org.openrdf.model.URI;
import org.openrdf.query.algebra.QueryModelVisitor;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.UnaryTupleOperator;

import java.util.*;

/**
 * Created by angel on 9/30/14.
 */
public class Plan extends UnaryTupleOperator {

    private Set<TupleExpr> id;

    private Map<String, Collection<URI>> schemas = new HashMap<String, Collection<URI>>();

    private PlanProperties properties;

    //public Plan(TupleExpr arg) { super(arg); }

    public Plan(Set<TupleExpr> id, TupleExpr arg) {
        super(arg);
        this.id = id;
        properties = PlanProperties.defaultProperties();
    }

    public Set<TupleExpr> getKey() { return this.id; }

    public PlanProperties getProperties() { return properties; }

    public void setProperties(PlanProperties properties) { this.properties = properties; }

    public <X extends Exception> void visit(QueryModelVisitor<X> xQueryModelVisitor) throws X {
        //getArg().visit(xQueryModelVisitor);
        xQueryModelVisitor.meetOther(this);
    }

    public String getSignature()
    {
        StringBuilder sb = new StringBuilder(128);

        sb.append(super.getSignature());
        sb.append("(cost=" + getProperties().getCost().toString() +")");
        return sb.toString();
    }
}
