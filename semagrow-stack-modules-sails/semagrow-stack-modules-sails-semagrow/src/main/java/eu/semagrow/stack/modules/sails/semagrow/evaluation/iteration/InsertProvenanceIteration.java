package eu.semagrow.stack.modules.sails.semagrow.evaluation.iteration;

import eu.semagrow.stack.modules.sails.semagrow.algebra.ProvenanceValue;
import eu.semagrow.stack.modules.sails.semagrow.evaluation.EvaluationStrategyImpl;
import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.ConvertingIteration;
import org.openrdf.model.Value;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

/**
 * Created by angel on 6/11/14.
 */
public class InsertProvenanceIteration extends ConvertingIteration<BindingSet,BindingSet,QueryEvaluationException> {

    private ProvenanceValue provenance;

    public InsertProvenanceIteration(CloseableIteration<BindingSet,QueryEvaluationException> iter,
            ProvenanceValue provenance) {
            super(iter);
            this.provenance = provenance;
    }

    @Override
    protected BindingSet convert(BindingSet bindings)
        throws QueryEvaluationException
    {
        QueryBindingSet newBindings = new QueryBindingSet();
        ProvenanceValue newProvenance = provenance;

        for (Binding b : bindings) {
            if (b.getName().equals(EvaluationStrategyImpl.provenanceField)) {
                Value oldProvenance = b.getValue();
                if (oldProvenance instanceof ProvenanceValue) {
                    newProvenance = mergeProvenance((ProvenanceValue)oldProvenance, newProvenance);
                }
            } else {
                newBindings.addBinding(b);
            }
        }
        newBindings.setBinding(EvaluationStrategyImpl.provenanceField, newProvenance);

        return newBindings;
    }

    protected ProvenanceValue mergeProvenance(ProvenanceValue oldP, ProvenanceValue P) {
        ProvenanceValue tmp = new ProvenanceValue(oldP);
        tmp.merge(P);
        return tmp;
    }
}