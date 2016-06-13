package eu.semagrow.core.impl.evaluation.util;

import eu.semagrow.core.eval.BindingSetOps;
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

public class BindingSetOpsImpl implements BindingSetOps {

    private static BindingSetOpsImpl defaultImpl;

    private BindingSetOpsImpl() { }

    public static BindingSetOps getInstance() {
        if (defaultImpl == null)
            defaultImpl = new BindingSetOpsImpl();
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

    /**
     * Returns a subset of vars with the variable names that appear in bindings.
     * @param vars
     * @param bindings
     * @return
     */
    @Override
    public Collection<String> projectNames(Collection<String> vars, BindingSet bindings) {
        Set<String> names = new HashSet<String>();

        for (String varName : vars) {
            if (bindings.hasBinding(varName))
                names.add(varName);
        }
        return names;
    }

    @Override
    public boolean hasBNode(BindingSet bindings) {

        for (Binding b : bindings) {
            if (b.getValue() instanceof BNode) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns true if the value of every binding name on first agrees with
     * the value of the binding name (if exists) on the second bindingset
     * @param first
     * @param second
     * @return true or false
     */
    @Override
    public boolean agreesOn(BindingSet first, BindingSet second) {

        for (Binding b : first) {
            Value v = second.getValue(b.getName());
            if (v != null && !v.equals(b.getValue()))
                return false;
        }
        return true;
    }
}
