package org.semagrow.plan.rel.sparql;

import com.google.common.base.Preconditions;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;

/**
 * Created by angel on 13/7/2017.
 */
public class SparqlService
        extends ConverterImpl
        implements SparqlRel
{
    private String ref;

    protected SparqlService(RelOptCluster cluster, RelTraitSet traits, RelNode child) {
        super(cluster, ConventionTraitDef.INSTANCE, traits, child);

        assert getConvention() instanceof SparqlConvention;
        assert getInput().getConvention() instanceof SparqlConvention;
        SparqlConvention conv = (SparqlConvention) getInput().getConvention();
        this.ref = Preconditions.checkNotNull(conv.getEndpoint());
    }

    public SparqlImplementor.Result implement(SparqlImplementor implementor) { return implementor.implement(this); }

}
