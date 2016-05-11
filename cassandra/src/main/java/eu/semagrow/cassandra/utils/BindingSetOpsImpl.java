package eu.semagrow.cassandra.utils;

import eu.semagrow.core.eval.BindingSetOps;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

import java.util.Collection;

/**
 * Created by antonis on 15/4/2016.
 */
public class BindingSetOpsImpl implements BindingSetOps {

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
    public BindingSet project(Collection<String> vars, BindingSet bindings, BindingSet parent) {
        return null;
    }

    @Override
    public Collection<String> projectNames(Collection<String> vars, BindingSet bindings) {
        return null;
    }

    @Override
    public boolean hasBNode(BindingSet bindings) {
        return false;
    }

    @Override
    public boolean agreesOn(BindingSet first, BindingSet second) {
        return false;
    }
}
