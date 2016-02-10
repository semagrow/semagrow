package eu.semagrow.core.impl.evaluation.iteration;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.ConvertingIteration;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

import java.util.Iterator;
import java.util.List;

/**
 * Created by angel on 6/5/14.
 */
public class InsertUnionBindingsIteration
        extends ConvertingIteration<BindingSet, BindingSet, QueryEvaluationException> {

    protected final List<BindingSet> bindings;

    public InsertUnionBindingsIteration(CloseableIteration<BindingSet,
            QueryEvaluationException> iter, List<BindingSet> bindings) {
        super(iter);
        this.bindings = bindings;
    }

    @Override
    protected BindingSet convert(BindingSet bIn) throws QueryEvaluationException {
        QueryBindingSet res = new QueryBindingSet();
        int bIndex = -1;
        Iterator<Binding> bIter = bIn.iterator();
        while (bIter.hasNext()) {
            Binding b = bIter.next();
            String name = b.getName();
            bIndex = Integer.parseInt(name.substring(name.lastIndexOf("_")+1));
            res.addBinding(name.substring(0, name.lastIndexOf("_")), b.getValue());
        }
        if (bIndex > -1)
            res.addAll( bindings.get(bIndex));
        return res;
    }
}
