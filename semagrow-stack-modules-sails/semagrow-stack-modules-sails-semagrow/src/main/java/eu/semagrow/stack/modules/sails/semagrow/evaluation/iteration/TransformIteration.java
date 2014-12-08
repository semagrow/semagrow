package eu.semagrow.stack.modules.sails.semagrow.evaluation.iteration;

import eu.semagrow.stack.modules.alignment.Transformer;
import info.aduna.iteration.ConvertingIteration;
import info.aduna.iteration.Iteration;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

import java.util.Map;

/**
 * Created by angel on 5/29/14.
 */
public class TransformIteration extends
        ConvertingIteration<BindingSet,BindingSet,QueryEvaluationException> {

    private Map<String, Transformer<URI,URI>> transfomers;

    public TransformIteration(Map<String, Transformer<URI,URI>> transformers,
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

            if (v instanceof URI) {
                Transformer<URI, URI> t = getTransformer(bindingName);
                if (t != null) {
                    URI transformed = t.transform((URI) v);
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

    private Transformer<URI,URI> getTransformer(String name) {
        return transfomers.get(name);
    }
}
