package org.semagrow.evaluation.util;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by angel on 15/6/2016.
 */
public class BindingSetUtil {

    /**
     * Returns a subset of vars with the variable names that appear in bindings.
     * @param vars
     * @param bindings
     * @return
     */

    public static Collection<String> projectNames(Collection<String> vars, BindingSet bindings) {
        Set<String> names = new HashSet<String>();

        for (String varName : vars) {
            if (bindings.hasBinding(varName))
                names.add(varName);
        }
        return names;
    }

    public static boolean hasBNode(BindingSet bindings) {

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
    public static boolean agreesOn(BindingSet first, BindingSet second) {

        for (Binding b : first) {
            Value v = second.getValue(b.getName());
            if (v != null && !v.equals(b.getValue()))
                return false;
        }
        return true;
    }
}
