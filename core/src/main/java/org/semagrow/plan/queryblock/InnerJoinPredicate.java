package org.semagrow.plan.queryblock;


import org.eclipse.rdf4j.query.algebra.Compare;
import org.eclipse.rdf4j.query.algebra.ValueExpr;

/**
 * @author acharal
 */
public class InnerJoinPredicate extends BinaryPredicate {

    public InnerJoinPredicate(Quantifier.Var from, Quantifier.Var to) {
        super(from, to);
    }

    public void replaceVarWith(Quantifier.Var v1, Quantifier.Var v2) {

        if (getFrom().equals(v1))
            setFrom(v2);

        if (getTo().equals(v1))
            setTo(v2);
    }

    public ValueExpr asExpr() {
        return new Compare(getFrom(), getTo(), Compare.CompareOp.EQ);
    }

}
