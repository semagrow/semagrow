package org.semagrow.plan;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.algebra.QueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.UnaryTupleOperator;

import java.util.*;

/**
 * Created by angel on 9/30/14.
 */
public class Plan extends UnaryTupleOperator {

    private Set<TupleExpr> id;

    private Map<String, Collection<IRI>> schemas = new HashMap<String, Collection<IRI>>();

    private PlanProperties properties;

    //public Plan(TupleExpr arg) { super(arg); }

    public Plan(Set<TupleExpr> id, TupleExpr arg) {
        super(arg);
        this.id = id;
        //properties = SimplePlanProperties.defaultProperties();
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

    public static Plan create(Set<TupleExpr> key, TupleExpr expr) {
        Plan p = new Plan(key, expr);
        return p;
    }

}
