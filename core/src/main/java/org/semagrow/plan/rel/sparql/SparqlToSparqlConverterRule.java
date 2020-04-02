package org.semagrow.plan.rel.sparql;

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

/**
 * Created by angel on 13/7/2017.
 */
public class SparqlToSparqlConverterRule extends ConverterRule {

    SparqlToSparqlConverterRule(SparqlConvention out, SparqlConvention in) {
        super(SparqlRel.class, out, in,
               "SPARQLtoSPARQLConverterRule");
    }

    @Override public RelNode convert(RelNode rel) {

        SparqlConvention out = (SparqlConvention) getOutTrait();
        SparqlConvention in  = (SparqlConvention) getInTrait();

        RelTraitSet traits = rel.getTraitSet().replace(out);

        if (out.getEndpoint().equals(in.getEndpoint()))
        {
            if (out.getDialect().isCompatible(in.getDialect()))
                return rel.copy(traits, rel.getInputs());
            else {
                // NOTE: same endpoint but different SPARQL version.
                // it is not safe (or is it?) to convert from SPARQL11 to SPARQL10 since rel may
                // contain nodes only for SPARQL11.
                return null;
            }
        } else {

            if (out.getDialect().supportsFederatedQuery()) {
                return new SparqlService(rel.getCluster(), traits, rel);
            } else {
                return null;
            }
        }
    }
}
