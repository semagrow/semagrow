package org.semagrow.plan.rel.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rex.RexLiteral;
import org.semagrow.plan.rel.schema.RelOptDataset;
import org.semagrow.plan.rel.StatementScan;

import java.util.List;

/**
 * Created by angel on 12/7/2017.
 */
public class LogicalStatementScan extends StatementScan {


    protected LogicalStatementScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptDataset dataset) {
        super(cluster, traitSet, dataset);
    }

    static public LogicalStatementScan create(RelOptCluster cluster,
                                 RelOptDataset dataset)
    {
        RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);

        return new LogicalStatementScan(cluster, traitSet, dataset);
    }
}
