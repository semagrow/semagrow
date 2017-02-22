package org.semagrow.plan.queryblock;

import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Created by angel on 6/9/2016.
 */
public class ThetaJoinPredicate implements Predicate {

    private ValueExpr theta;

    public ThetaJoinPredicate(ValueExpr theta) {
        this.theta = theta;
    }

    public ValueExpr asExpr() { return theta; }

    public Collection<Quantifier.Var> getVariables() {
        return QuantifierCollector.process(theta);
    }

    @Override
    public Collection<Quantifier> getEL() {
        return QuantifierCollector.process(theta).stream()
                .map(v -> v.getQuantifier()).collect(Collectors.toSet());
    }

    @Override
    public Collection<Quantifier> getEEL() {
        return getEL();
    }

    @Override
    public void replaceVarWith(Quantifier.Var v1, Quantifier.Var v2) {
        replaceWith(v1, v2);
    }

    public void replaceWith(Quantifier.Var v1, ValueExpr e) {
        theta.visit(new AbstractQueryModelVisitor<RuntimeException>() {
            @Override
            public void meetNode(QueryModelNode node) throws RuntimeException {
                if (node instanceof Quantifier.Var) {
                    Quantifier.Var v = (Quantifier.Var)node;
                    if (v.equals(v1))
                        node.replaceWith(e);
                }
                super.meetNode(node);
            }
        });
    }
}
