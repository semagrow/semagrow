package org.semagrow.plan.queryblock;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Created by angel on 9/9/2016.
 */
public abstract class BinaryPredicate implements Predicate {

    private Quantifier.Var from;

    private Quantifier.Var to;

    public BinaryPredicate(Quantifier.Var from, Quantifier.Var to) {
        setFrom(from);
        setTo(to);
    }

    public void setFrom(Quantifier.Var q) {
        if (q == null)
            throw new IllegalArgumentException("Quantifier cannot be null");
        from = q;
    }

    public void setTo(Quantifier.Var q)   {
        if (q == null)
            throw new IllegalArgumentException("Quantifier cannot be null");
        to = q;
    }

    public Collection<Quantifier.Var> getVariables() {
        return Arrays.asList(getFrom(), getTo());
    }

    public Quantifier.Var getFrom() { return from; }

    public Quantifier.Var getTo() { return to; }

    public Collection<Quantifier> getEL() {
        return Arrays.asList(from.getQuantifier(), to.getQuantifier()).stream()
                .distinct().collect(Collectors.toSet());
    }

    public Collection<Quantifier> getEEL() { return getEL(); }

}
