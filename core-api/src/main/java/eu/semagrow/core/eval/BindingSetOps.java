package eu.semagrow.core.eval;

import org.openrdf.query.BindingSet;

import java.util.Collection;

/**
 * Created by angel on 30/3/2016.
 */
public interface BindingSetOps {
    BindingSet merge(BindingSet first, BindingSet second);

    BindingSet project(Collection<String> vars, BindingSet bindings);

    BindingSet project(Collection<String> vars,
                       BindingSet bindings,
                       BindingSet parent);

    Collection<String> projectNames(Collection<String> vars, BindingSet bindings);

    boolean hasBNode(BindingSet bindings);

    boolean agreesOn(BindingSet first, BindingSet second);
}
