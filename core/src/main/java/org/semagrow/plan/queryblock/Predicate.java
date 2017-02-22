package org.semagrow.plan.queryblock;

import org.eclipse.rdf4j.query.algebra.ValueExpr;

import java.util.Collection;

/**
 * Created by angel on 6/9/2016.
 */
public interface Predicate {

    /**
     * Returns the eligible list of {@link Quantifier} related
     * in this query predicate.
     * @return a collection of related {@link Quantifier}
     */
    Collection<Quantifier> getEL();

    /**
     * Returns the extended eligible list of {@link Quantifier}s
     * which is a superset of eligible list
     * @see this.getEL()
     * @return a collection of related {@link Quantifier}
     */
    Collection<Quantifier> getEEL();

    Collection<Quantifier.Var> getVariables();

    void replaceVarWith(Quantifier.Var var1, Quantifier.Var var2);

    ValueExpr asExpr();
}
