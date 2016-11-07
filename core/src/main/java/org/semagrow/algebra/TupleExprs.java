package org.semagrow.algebra;

import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

import java.util.HashSet;
import java.util.Set;

/**
 * A set of various utility functions concerning {@link TupleExpr}s
 * @author acharal
 */
public class TupleExprs {

    /**
     * Computes the set of the unbounded variables of a {@link TupleExpr}
     * @param expr a tuple expression
     * @return a set of variable names
     */
    public static Set<String> getFreeVariables(TupleExpr expr){
        final Set<String> res = new HashSet<String>();
        expr.visit(new AbstractQueryModelVisitor<RuntimeException>() {

            @Override
            public void meet(Var node)
                    throws RuntimeException {
                // take only real vars, i.e. ignore blank nodes
                if (!node.hasValue() && !node.isAnonymous())
                    res.add(node.getName());
            }
        });
        return res;
    }


}
