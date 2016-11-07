package org.semagrow.plan.queryblock;

import org.eclipse.rdf4j.query.algebra.Compare;
import org.eclipse.rdf4j.query.algebra.ValueExpr;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * @author acharal
 */
public class LeftJoinPredicate extends BinaryPredicate {

    private Collection<Quantifier> eel;
    private Optional<ValueExpr> cond;

    public LeftJoinPredicate(Quantifier.Var from, Quantifier.Var to, ValueExpr cond)
    {
        this(from, to, Optional.of(cond));
    }

    public LeftJoinPredicate(Quantifier.Var from, Quantifier.Var to) {
        this(from, to, Optional.empty());
    }

    public LeftJoinPredicate(Quantifier.Var from, Quantifier.Var to, Optional<ValueExpr> cond) {
        super(from,to);
        this.cond = cond;
        setEEL(getEL());
    }

    public Collection<Quantifier> getEL() {
        Set<Quantifier> coll = new HashSet<>(super.getEL());

        if (cond.isPresent()) {
            QuantifierCollector.process(cond.get()).stream()
                    .map(q -> q.getQuantifier())
                    .forEach( q -> {
                        coll.add(q);
                    });
        }
        return coll;
    }

    public void setEEL(Collection<Quantifier> eel) { this.eel = eel; }

    public Collection<Quantifier> getEEL() { return eel; }

    public ValueExpr asExpr() {
        return new Compare(getFrom(), getTo(), Compare.CompareOp.EQ);
    }

    public void replaceVarWith(Quantifier.Var v1, Quantifier.Var v2) {

        if (getFrom().equals(v1))
            setFrom(v2);

        if (getTo().equals(v1))
            setTo(v2);
    }

}
