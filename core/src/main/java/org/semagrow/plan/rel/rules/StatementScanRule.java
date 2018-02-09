package org.semagrow.plan.rel.rules;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.semagrow.plan.rel.StatementScan;
import org.semagrow.plan.rel.logical.LogicalStatementScan;
import org.semagrow.plan.rel.schema.RelOptDataset;

public class StatementScanRule extends RelOptRule {

    public static StatementScanRule INSTANCE = new StatementScanRule();

    private StatementScanRule() {
        super(operand(LogicalStatementScan.class, any()));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final StatementScan scan = call.rel(0);
        RelNode newRel =
                scan.getDataset().toRel(
                        new RelOptDataset.ToRelContext() {
                            @Override
                            public RelOptCluster getCluster() {
                                return scan.getCluster();
                            }
                        }
                );
        call.transformTo(newRel);
    }
}
