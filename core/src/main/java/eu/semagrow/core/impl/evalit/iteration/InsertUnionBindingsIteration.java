package eu.semagrow.core.impl.evalit.iteration;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.ConvertingIteration;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;

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
