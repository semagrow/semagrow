package org.semagrow.plan.rel.logical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.semagrow.plan.rel.Service;

/**
 * Created by angel on 13/7/2017.
 */
public class LogicalService extends Service {



    protected LogicalService(RelOptCluster cluster,
                             RelTraitSet traits,
                             RelNode input,
                             RexNode ref,
                             boolean silent) {
        super(cluster, traits, input, ref, silent);
    }

    @Override
    public Service copy(RelTraitSet traitSet, RelNode input, RexNode ref, boolean silent) {
        return new LogicalService(input.getCluster(), traitSet, input, ref, silent);
    }

    static public LogicalService create(RelNode input, RexNode ref) {
        return create(input, ref, false);
    }

    static public LogicalService create(RelNode input, RexNode ref, boolean silent) {
        return new LogicalService(input.getCluster(), input.getTraitSet(), input, ref, silent);
    }
}
