package org.semagrow.plan.rel.sparql;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.semagrow.plan.rel.semagrow.SemagrowConvention;

/**
 * Created by angel on 17/7/2017.
 */
public class SparqlToSemagrowConverterRule extends ConverterRule {

    SparqlToSemagrowConverterRule(SparqlConvention out) {
        super(RelNode.class, out, SemagrowConvention.INSTANCE,
                "SparqltoSemagrowConverterRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
        return new SparqlToSemagrowConverter(rel.getCluster(),
                rel.getTraitSet().replace(SemagrowConvention.INSTANCE),
                rel);
    }

}
