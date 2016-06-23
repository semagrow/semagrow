package org.semagrow.evaluation.util;

import org.semagrow.evaluation.BindingSetOps;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Various static functions for {@link BindingSet} handling.
 *
 * @author Angelos Charalambidis
 */

public class SimpleBindingSetOps implements BindingSetOps {

    private static SimpleBindingSetOps defaultImpl;

    private SimpleBindingSetOps() { }

    public static BindingSetOps getInstance() {
        if (defaultImpl == null)
            defaultImpl = new SimpleBindingSetOps();
        return defaultImpl;
    }

    /**
     * Merge two bindingSet into one. If some bindings of the second set refer to
     * variable names of the first binding then prefer the bindings of the first set.
     * @param first
     * @param second
     * @return A binding set that contains the union of the variable bindings of first and second set.
     */
    @Override
    public BindingSet merge(BindingSet first, BindingSet second) {
        QueryBindingSet result = new QueryBindingSet();

        for (Binding b : first) {
            if (!result.hasBinding(b.getName()))
                result.addBinding(b);
        }

        for (String name : second.getBindingNames()) {
            Binding b = second.getBinding(name);
            if (!result.hasBinding(name)) {
                result.addBinding(b);
            }
        }
        return result;
    }

    /**
     * Project a binding set to a potentially smaller binding set that
     * contain only the variable bindings that are in vars set.
     * @param bindings
     * @param vars
     * @return
     */
    @Override
    public BindingSet project(Collection<String> vars, BindingSet bindings) {
        QueryBindingSet q = new QueryBindingSet();

        for (String varName : vars) {
            Binding b = bindings.getBinding(varName);
            if (b != null) {
                q.addBinding(b);
            }
        }
        return q;
    }

    @Override
    public BindingSet project(Collection<String> vars,
                              BindingSet bindings,
                              BindingSet parent)
    {
        BindingSet result = merge(bindings, parent);// new QueryBindingSet(parent);
        return project(vars, result);
    }

}
