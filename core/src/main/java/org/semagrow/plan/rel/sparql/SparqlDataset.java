package org.semagrow.plan.rel.sparql;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.semagrow.plan.rel.schema.AbstractDataset;
import org.semagrow.plan.rel.schema.RelOptDataset;


public class SparqlDataset extends AbstractDataset {

    private SparqlConvention convention;

    protected SparqlDataset(SparqlConvention convention) {
        this.convention = convention;
    }

    @Override
    public RelNode toRel(RelOptDataset.ToRelContext context, RelOptDataset dataset) {
        RelTraitSet traitSet = RelTraitSet.createEmpty().plus(getConvention());

        return SparqlStatementPattern.create(context.getCluster(),
                dataset,
                traitSet,
                getRowType(context.getCluster().getTypeFactory()),
                ImmutableMap.of(),
                null);
    }

    protected SparqlConvention getConvention() {
        return convention;
    }
}
