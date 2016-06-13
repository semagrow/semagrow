package eu.semagrow.core.impl.evalit.iteration;

import eu.semagrow.core.impl.alignment.Transformer;
import org.eclipse.rdf4j.common.iteration.ConvertingIteration;
import org.eclipse.rdf4j.common.iteration.Iteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;

import java.util.Map;

/**
 * Created by angel on 5/29/14.
 */
public class TransformIteration extends
        ConvertingIteration<BindingSet,BindingSet,QueryEvaluationException> {

    private Map<String, Transformer<IRI,IRI>> transfomers;

    public TransformIteration(Map<String, Transformer<IRI,IRI>> transformers,
                              Iteration<? extends BindingSet, ? extends QueryEvaluationException> iter)
    {
        super(iter);
        this.transfomers = transformers;
    }

    @Override
    protected BindingSet convert(BindingSet bindings)
            throws QueryEvaluationException
    {
        QueryBindingSet bindingSet = new QueryBindingSet();
        for (String bindingName : bindings.getBindingNames()) {
            Value v = bindings.getValue(bindingName);

            if (v instanceof IRI) {
                Transformer<IRI, IRI> t = getTransformer(bindingName);
                if (t != null) {
                    IRI transformed = t.transform((IRI) v);
                    if (transformed != null) {
                        bindingSet.setBinding(bindingName, transformed);
                        continue;
                    }
                }
            }

            bindingSet.setBinding(bindingName, v);
        }
        return bindingSet;
    }

    private Transformer<IRI,IRI> getTransformer(String name) {
        return transfomers.get(name);
    }
}
