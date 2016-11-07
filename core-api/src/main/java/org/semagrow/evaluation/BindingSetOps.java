package org.semagrow.evaluation;

import org.eclipse.rdf4j.query.BindingSet;

import java.util.Collection;

/**
 * The interface of the necessary operation between {@link BindingSet}s
 * @author acharal
 */
public interface BindingSetOps {

    /**
     * Merges two {@link BindingSet}s into a single one that will contain
     * all the bindings of both. If there exist {@link org.eclipse.rdf4j.query.Binding}
     * of the same variable in both {@link BindingSet}s then the one of the first
     * {@link BindingSet} will be kept and the other one will be discarded.
     * @param first the first {@link BindingSet}
     * @param second the second {@link BindingSet}
     * @return the merged {@link BindingSet}
     */
    BindingSet merge(BindingSet first, BindingSet second);

    /**
     * Creates a {@link BindingSet} that contain only the {@link org.eclipse.rdf4j.query.Binding}s
     * that refer to variables in {@code vars}
     * @param vars the set of variable names
     * @param bindings the initial {@link BindingSet}
     * @return
     */
    BindingSet project(Collection<String> vars, BindingSet bindings);


    BindingSet project(Collection<String> vars, BindingSet bindings, BindingSet parent);

}
