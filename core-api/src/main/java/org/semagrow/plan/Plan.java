package org.semagrow.plan;

import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.helpers.VarNameCollector;

import java.util.HashSet;
import java.util.Set;

/**
 * A query plan is a subtree of physical operators that contain the
 * necessary information needed for the evaluation of the tree.
 * <p>
 * A Plan operator is essentially an extension of a {@link UnaryTupleOperator}
 * that wraps a {@link TupleExpr} and has an attached {@link PlanProperties}.
 *
 * @see PlanProperties
 * @see TupleExpr
 * @see UnaryTupleOperator
 * @author acharal
 */
public class Plan extends UnaryTupleOperator {

    private PlanProperties properties;

    public Plan(TupleExpr arg) {
        super(arg);
        setProperties(new PlanProperties());
    }

    public Plan(TupleExpr arg, PlanProperties props) {
        super(arg);
        setProperties(props);
    }

    public PlanProperties getProperties() { return properties; }

    public void setProperties(PlanProperties properties) { this.properties = properties; }

    public Set<String> getOutputVariables() {
        return VarNameCollector.process(getArg());
    }

    public boolean hasDuplicates() { return true; }

    public <X extends Exception> void visit(QueryModelVisitor<X> xQueryModelVisitor) throws X {
        //getArg().visit(xQueryModelVisitor);
        xQueryModelVisitor.meetOther(this);
    }

    public String getSignature()
    {
        StringBuilder sb = new StringBuilder(128);

        sb.append(super.getSignature());

        if (getProperties().getSite() != null)
            sb.append("@" + getProperties().getSite());

        sb.append("[");
        if (getProperties().getCost() != null)
            sb.append("costs " + getProperties().getCost().toString() + " ");

        sb.append(getProperties().getCardinality()  + " tuples");

        sb.append("]");

        return sb.toString();
    }

    private class OutputVarCollector extends AbstractQueryModelVisitor<RuntimeException> {

        private Set<String> variables = new HashSet<>();


        @Override
        public void meet(Var v) {
            if (!v.isConstant())
                variables.add(v.getName());
        }

        @Override
        public void meet(Projection p) {
            Set<String> vars = p.getProjectionElemList().getTargetNames();
            variables.addAll(vars);
        }

        @Override
        public void meet(Filter f) {
            // skips visiting the condition of filter.
            f.getArg().visit(this);
        }

        @Override
        public void meetOther(QueryModelNode node) {
            if (node instanceof Plan) {
                variables.addAll(((Plan) node).getOutputVariables());
            } else {
                super.meetOther(node);
            }
        }
    }

}
