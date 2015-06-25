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
public class InsertValuesBindingsIteration extends
    ConvertingIteration<BindingSet, BindingSet, QueryEvaluationException> {

    public static final String INDEX_BINDING_NAME = "__rowIdx";
    protected final List<BindingSet> bindings;

    public InsertValuesBindingsIteration(
            CloseableIteration<BindingSet, QueryEvaluationException> iter,
            List<BindingSet> bindings) {
        super(iter);
        this.bindings = bindings;
    }

    @Override
    protected BindingSet convert(BindingSet bIn) throws QueryEvaluationException {

        // overestimate the capacity
        QueryBindingSet res = new QueryBindingSet(bIn.size() + bindings.size());

        int bIndex = -1;
        Iterator<Binding> bIter = bIn.iterator();
        while (bIter.hasNext()) {
            Binding b = bIter.next();
            String name = b.getName();
            if (name.equals(INDEX_BINDING_NAME)) {
                bIndex = Integer.parseInt(b.getValue().stringValue());
                continue;
            }
            res.addBinding(b.getName(), b.getValue());
        }

        // should never occur: in such case we would have to create the cross product (which
        // is dealt with in another place)
        if (bIndex == -1)
            throw new QueryEvaluationException("Invalid join. Probably this is due to non-standard behavior of the SPARQL endpoint. " +
                    "Please report to the developers.");

        res.addAll(bindings.get(bIndex));
        return res;
    }
}
