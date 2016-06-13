package eu.semagrow.core.impl.evalit.iteration;

import eu.semagrow.core.impl.plan.ops.ProvenanceValue;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.ConvertingIteration;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;

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