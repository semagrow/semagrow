package eu.semagrow.stack.modules.sails.semagrow.evaluation.iteration;

import info.aduna.iteration.FilterIteration;
import info.aduna.iteration.Iteration;
import org.openrdf.model.BNode;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;

import java.util.Set;

/**
 * Created by angel on 17/4/2015.
 */
public class BNodeFilteringIteration<X extends Exception> extends FilterIteration<BindingSet, X> {


    private Set<String> vars;

    public BNodeFilteringIteration(Set<String> variables, Iteration<? extends BindingSet, ? extends X> iter) {
        super(iter);
        this.vars = variables;
    }

    @Override
    protected boolean accept(BindingSet bindings) throws X {

        for (Binding b : bindings){
            if (vars.contains(b.getName()) &&  b.getValue() != null && b.getValue() instanceof BNode) {
                return false;
            }
        }

        return true;
    }
}
