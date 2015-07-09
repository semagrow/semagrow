package eu.semagrow.core.impl.evaluation.util;

import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by angel on 9/7/2015.
 */
public class BindingSetUtils {


    /**
     * Merge two bindingSet into one. If some bindings of the second set refer to
     * variable names of the first binding then prefer the bindings of the first set.
     * @param first
     * @param second
     * @return A binding set that contains the union of the variable bindings of first and second set.
     */
    public static BindingSet merge(BindingSet first, BindingSet second) {
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
    public static BindingSet project( Collection<String> vars, BindingSet bindings) {
        QueryBindingSet q = new QueryBindingSet();

        for (String varName : vars) {
            Binding b = bindings.getBinding(varName);
            if (b != null) {
                q.addBinding(b);
            }
        }
        return q;
    }

    public static BindingSet project(Collection<String> vars,
                                     BindingSet bindings,
                                     BindingSet parent)
    {
        QueryBindingSet result = new QueryBindingSet(parent);
        return project(vars, result);
    }

    /**
     * Returns a subset of vars with the variable names that appear in bindings.
     * @param vars
     * @param bindings
     * @return
     */
    public static Collection<String> projectNames(Collection<String>vars, BindingSet bindings) {
        Set<String> names = new HashSet<String>();

        for (String varName : vars) {
            if (bindings.hasBinding(varName))
                names.add(varName);
        }
        return names;
    }

}
