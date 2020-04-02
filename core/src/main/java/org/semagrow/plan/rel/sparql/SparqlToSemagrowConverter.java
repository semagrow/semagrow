package org.semagrow.plan.rel.sparql;

import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.semagrow.plan.rel.semagrow.SemagrowRel;

import java.util.List;

/**
 * Created by angel on 17/7/2017.
 */
public class SparqlToSemagrowConverter extends ConverterImpl
        implements SemagrowRel
{
    protected SparqlToSemagrowConverter(RelOptCluster cluster, RelTraitSet traits, RelNode child) {
        super(cluster, ConventionTraitDef.INSTANCE, traits, child);

        assert child.getConvention() instanceof SparqlConvention;

    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new SparqlToSemagrowConverter(getCluster(), traitSet, sole(inputs));
    }


}
